(ns com.dept24c.vivo.utils
  (:require
   #?(:cljs [cljs.reader :as reader])
   #?(:clj [clojure.edn :as edn])
   #?(:cljs [clojure.pprint :as pprint])
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.lancaster :as l]
   #?(:clj [puget.printer :refer [cprint]]))
  #?(:cljs
     (:require-macros
      [com.dept24c.vivo.utils :refer [sym-map]])))

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defn pprint [x]
  #?(:clj (cprint x)
     :cljs (pprint/pprint x)))

(defn ex-msg [e]
  #?(:clj (.toString ^Exception e)
     :cljs (.-message e)))

(defn ex-stacktrace [e]
  #?(:clj (clojure.string/join "\n" (map str (.getStackTrace ^Exception e)))
     :cljs (.-stack e)))

(defn ex-msg-and-stacktrace [e]
  (str "\nException:\n" (ex-msg e) "\nStacktrace:\n" (ex-stacktrace e)))

(defn current-time-ms []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(defn edn->str [edn]
  (pr-str edn))

(defn str->edn [s]
  #?(:clj (edn/read-string s)
     :cljs (reader/read-string s)))

;;;;;;;;;;;;;;;;;;;; Schemas ;;;;;;;;;;;;;;;;;;;;

(def fp-schema l/long-schema)
(def pcf-schema l/string-schema)
(def sub-id-schema l/string-schema)

(l/def-record-schema skeyword-schema
  [:ns (l/maybe l/string-schema)]
  [:name l/string-schema])

(l/def-union-schema spath-item-schema
  skeyword-schema
  l/string-schema
  l/int-schema)

(l/def-array-schema spath-schema
  spath-item-schema)

(l/def-enum-schema op-schema
  {:key-ns-type :none}
  :set :remove :insert-before :insert-after :plus :minus :multiply :divide :mod)

(l/def-record-schema subscribe-arg-schema
  [:sub-id sub-id-schema]
  [:sub-map (l/map-schema spath-schema)])

(l/def-record-schema svalue-schema
  [:encoded-v l/bytes-schema]
  [:fp l/long-schema])

(l/def-record-schema update-command-schema
  [:path spath-schema]
  [:op op-schema]
  [:arg (l/maybe svalue-schema)])

(l/def-record-schema update-state-arg-schema
  [:tx-info-str (l/maybe l/string-schema)]
  [:update-commands (l/array-schema update-command-schema)])

(l/def-record-schema notify-subscriber-arg-schema
  [:sub-id sub-id-schema]
  [:data-frame (l/map-schema svalue-schema)]
  [:tx-info-str (l/maybe l/string-schema)])

(def bsp-bs-protocol
  {:roles [:state-provider :server]
   :msgs {:update-state {:arg update-state-arg-schema
                         :ret l/boolean-schema
                         :sender :state-provider}
          :subscribe {:arg subscribe-arg-schema
                      :ret l/boolean-schema
                      :sender :state-provider}
          :unsubscribe {:arg sub-id-schema
                        :ret l/boolean-schema
                        :sender :state-provider}
          :notify-subscriber {:arg notify-subscriber-arg-schema
                              :sender :server}
          :request-pcf {:arg fp-schema
                        :ret pcf-schema
                        :sender :either}}})

(defn edn->svalue [state-schema path v]
  (let [schema (l/schema-at-path state-schema path)]
    #:svalue{:encoded-v (l/serialize schema v)
             :fp (l/fingerprint64 schema)}))

(defn make-<fp->wschema [<sender *fp->pcf]
  (fn [fp]
    (au/go
      (let [pcf (or (@*fp->pcf fp)
                    (let [pcf* (au/<? (<sender :request-pcf fp))]
                      (swap! *fp->pcf assoc fp pcf*)
                      pcf*))]
        (l/json->schema pcf)))))

(defn <svalue->edn [<fp->wschema state-schema path svalue]
  (au/go
    (let [rschema (l/schema-at-path state-schema path)
          {:svalue/keys [encoded-v fp]} svalue
          wschema (au/<? (<fp->wschema fp))]
      (l/deserialize wschema rschema encoded-v))))

(defn kw->skeyword [kw]
  #:skeyword{:ns (namespace kw)
             :name (name kw)})

(defn skeyword->kw [skw]
  (let [{:skeyword/keys [ns name]} skw]
    (keyword ns name)))

(defn path->spath [path]
  (reduce (fn [acc k]
            (conj acc (if (keyword? k)
                        (kw->skeyword k)
                        k)))
          [] path))

(defn spath->path [spath]
  (reduce (fn [acc k]
            (conj acc (if (map? k)
                        (skeyword->kw k)
                        k)))
          [] spath))

(defn update-array-sub? [len sub-i update-i* op]
  (let [update-i (if (nat-int? sub-i)
                   (if (nat-int? update-i*)
                     update-i*
                     (+ len update-i*))
                   (if (nat-int? update-i*)
                     (- update-i* len)
                     update-i*))]
    (if (= :set op)
      (= sub-i update-i)
      (let [new-i (if (= :insert-after op)
                    (if (nat-int? update-i)
                      (inc update-i)
                      update-i)
                    (if (nat-int? update-i)
                      update-i
                      (dec update-i)))]
        (if (nat-int? sub-i)
          (<= new-i sub-i)
          (>= new-i sub-i))))))

(defn relationship-info
  "Given two key sequences, return a vector of [relationship tail].
   Relationsip is one of :sibling, :parent, :child, or :equal.
   Tail is the keypath between the parent and child. Tail is only defined
   when relationship is :parent."
  [ksa ksb]
  (let [va (vec ksa)
        vb (vec ksb)
        len-a (count va)
        len-b (count vb)
        len-min (min len-a len-b)
        divergence-i (loop [i 0]
                       (if (and (< i len-min)
                                (= (va i) (vb i)))
                         (recur (inc i))
                         i))
        a-tail? (> len-a divergence-i)
        b-tail? (> len-b divergence-i)]
    (cond
      (and a-tail? b-tail?) [:sibling nil]
      a-tail? [:child nil]
      b-tail? [:parent (drop divergence-i ksb)]
      :else [:equal nil])))

(defn get-in-state
  "Custom get-in fn that checks types and normalizes negative keys.
   Returns a map with :norm-path and :val keys."
  [state path]
  (reduce (fn [{:keys [norm-path val] :as acc} k]
            (let [[k* val*] (cond
                              (or (keyword? k) (nat-int? k) (string? k))
                              [k (when val
                                   (val k))]

                              (and (int? k) (neg? k))
                              (let [k (when val
                                        (if (or (vector? val) (nil? val))
                                          (+ (count val) k)
                                          k))]
                                [k (when val
                                     (val k))])

                              :else
                              (throw
                               (ex-info
                                (str "Illegal key `" k "` in path `" path
                                     "`. Only integers, keywords, "
                                     "and strings are valid path keys.")
                                (sym-map k path))))]
              (-> acc
                  (update :norm-path conj k*)
                  (assoc :val val*))))
          {:norm-path []
           :val state}
          path))

(defn make-data-frame [sub-map state]
  (reduce-kv (fn [acc df-key path]
               (let [{:keys [val]} (get-in-state state path)]
                 (assoc acc df-key val)))
             {} sub-map))
#_
(defn update-sub? [sub-map update-path orig-v new-v]
  ;; orig-v and new-v are guaranteed to be different
  (reduce (fn [acc subscription-path]
            (let [[relationship sub-tail] (relationship-info
                                           update-path subscription-path)]
              (case relationship
                :equal (reduced true)
                :child (reduced true)
                :sibling false
                :parent (if (= (get-in orig-v sub-tail)
                               (get-in new-v sub-tail))
                          false
                          (reduced true)))))
          false (vals sub-map)))

#_
(defn get-change-info [get-in-state update-map subs]
  ;; TODO: Handle ordered update-map with in-process vals
  (let [path->vals (reduce
                    (fn [acc [path upex]]
                      (let [orig-v (get-in-state path)
                            new-v (upex/eval orig-v upex)]
                        (if (= orig-v new-v)
                          acc
                          (assoc acc path [orig-v new-v]))))
                    {} update-map)
        subs-to-update (fn [subs path orig-v new-v]
                         (reduce (fn [acc {:keys [sub-map] :as sub}]
                                   (if (update-sub? sub-map path orig-v new-v)
                                     (conj acc sub)
                                     acc))
                                 #{} subs))]
    (reduce-kv (fn [acc path [orig-v new-v]]
                 (-> acc
                     (update :state-updates #(assoc % path new-v))
                     (update :subs-to-update set/union
                             (subs-to-update subs path orig-v new-v))))
               {:state-updates {}
                :subs-to-update #{}}
               path->vals)))
#_
(defn update-state* [update-map get-in-state set-paths-in-state! subs]
  (let [{:vivo/keys [tx-info-str]} update-map
        update-map* (dissoc update-map :vivo/tx-info-str)
        {:keys [state-updates subs-to-update]} (get-change-info
                                                get-in-state update-map* subs)]
    (set-paths-in-state! state-updates)
    (doseq [{:keys [update-fn sub-map]} subs-to-update]
      (let [data-frame (cond-> (make-data-frame get-in-state sub-map)
                         tx-info-str (assoc :vivo/tx-info-str tx-info-str))]
        (update-fn data-frame)))
    true))

;;;;;;;;;;;;;;;;;;;; Platform detection ;;;;;;;;;;;;;;;;;;;;

(defn jvm? []
  #?(:clj true
     :cljs false))

(defn browser? []
  #?(:clj false
     :cljs (exists? js/navigator)))

(defn node? []
  #?(:clj false
     :cljs (boolean (= "nodejs" cljs.core/*target*))))

(defn platform-kw []
  (cond
    (jvm?) :jvm
    (node?) :node
    (browser?) :browser
    :else :unknown))
