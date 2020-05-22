(ns com.dept24c.vivo.client.subscriptions
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [clojure.string :as str]
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.react :as react]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as log]
   [weavejester.dependency :as dep]))

(defn get-non-numeric-part [path]
  (take-while #(not (number? %)) path))

(defn update-numeric? [updated-path sub-path op]
  ;; TODO: Improve this using op / normalized paths
  (let [u-front (get-non-numeric-part updated-path)
        s-front (get-non-numeric-part sub-path)]
    (if (or (not (seq u-front))
            (not (seq s-front)))
      true
      (let [[relationship _] (u/relationship-info u-front s-front)]
        (not= :sibling relationship)))))

(defn update-sub?* [update-infos sub-path]
  (reduce (fn [acc {:keys [norm-path op] :as update-info}]
            (cond
              (= [:vivo/subject-id] sub-path)
              (if (= [:vivo/subject-id] norm-path)
                (reduced true)
                false)

              (= [:vivo/subject-id] norm-path)
              (if (= [:vivo/subject-id] sub-path)
                (reduced true)
                false)

              (or (some number? norm-path)
                  (some number? sub-path))
              (if (update-numeric? norm-path sub-path op)
                (reduced true)
                false)

              :else
              (let [[relationship _] (u/relationship-info
                                      (or norm-path [])
                                      (or sub-path []))]
                (if (= :sibling relationship)
                  false
                  ;; TODO: Compare values here if :parent
                  (reduced true)))))
          false update-infos))

(defn transform-operators-in-sub-path [sub-path]
  (reduce (fn [acc k]
            (if (u/kw-ops k)
              (reduced acc)
              (conj acc k)))
          [] sub-path))

(defn update-sub? [update-infos sub-paths]
  (reduce
   (fn [acc sub-path]
     (when-not (sequential? sub-path)
       (throw (ex-info (str "`sub-path` must be seqential. Got: `"
                            sub-path "`.")
                       (u/sym-map sub-path))))
     (if (update-sub?* update-infos
                       (transform-operators-in-sub-path sub-path))
       (reduced true)
       false))
   false
   sub-paths))

(defn get-sub-names-to-update
  [db local-state update-infos *sub-name->info]
  (reduce-kv (fn [acc sub-name info]
               (let [{:keys [expanded-paths]} info
                     update? (update-sub? update-infos expanded-paths)]
                 (if update?
                   (conj acc sub-name)
                   acc)))
             #{} @*sub-name->info))

(defn order-by-lineage [sub-names-to-update *sub-name->info]
  (let [g (reduce
           (fn [acc sub-name]
             (let [{:keys [parents]} (@*sub-name->info sub-name)
                   update-parents (set/intersection parents
                                                    sub-names-to-update)]
               (if (empty? update-parents)
                 (dep/depend acc sub-name :vivo/root)
                 (reduce (fn [acc* parent]
                           (if (sub-names-to-update parent)
                             (dep/depend acc* sub-name parent)
                             acc*))
                         acc
                         update-parents))))
           (dep/graph)
           sub-names-to-update)]
    (->> (dep/topo-sort g)
         (filter #(not= :vivo/root %)))))

(defn resolve-symbols-in-path [state path]
  ;; TODO: Could optimize by storing info about non-dependent paths
  (reduce (fn [acc element]
            (conj acc (if-not (symbol? element)
                        element
                        (state element))))
          [] path))

(defn ks-at-path [kw state path prefix full-path]
  (let [coll (:val (commands/get-in-state state path prefix))]
    (cond
      (map? coll)
      (keys coll)

      (sequential? coll)
      (range (count coll))

      (nil? coll)
      []

      :else
      (throw
       (ex-info
        (str "`" kw "` is in the path, but "
             "there is not a collection at " path ".")
        {:full-path full-path
         :missing-collection-path path
         :value coll})))))

(defn count-at-path [state path prefix]
  (let [coll (:val (commands/get-in-state state path prefix))]
    (cond
      (or (map? coll) (sequential? coll))
      (count coll)

      (nil? coll)
      0

      :else
      (throw
       (ex-info
        (str "`:vivo/count` terminates path, but there is not a collection at "
             path ".")
        {:path path
         :value coll})))))

(defn do-concat [state path prefix]
  (let [seqs (:val (commands/get-in-state state path prefix))]
    (when (or (not (sequential? seqs))
              (not (sequential? (first seqs))))
      (throw
       (ex-info
        (str "`:vivo/concat` terminates path, but there "
             "is not a sequence of sequences at " path ".")
        {:path path
         :value seqs})))
    (apply concat seqs)))

(defn get-state-and-expanded-path [state path prefix]
  ;; TODO: Optimize this. Only traverse the path once.
  (let [last-path-k (last path)
        join? (u/has-join? path)
        wildcard-parent (-> (partition-by #(= :vivo/* %) path)
                            (first))
        wildcard? (not= path wildcard-parent)
        terminal-kw? (u/terminal-kw-ops last-path-k)
        ks-at-path* #(ks-at-path :vivo/* state % prefix path)]
    (cond
      (u/empty-sequence-in-path? path)
      [nil [path]]

      (and (not terminal-kw?) (not join?))
      (let [{:keys [norm-path val]} (commands/get-in-state state path prefix)]
        [val [norm-path]])

      (and terminal-kw? (not join?))
      (let [path* (butlast path)
            val (case last-path-k
                  :vivo/keys (ks-at-path :vivo/keys state path* prefix path)
                  :vivo/count (count-at-path state path* prefix)
                  :vivo/concat (do-concat state path* prefix))]
        [val [path*]])

      (and (not terminal-kw?) join?)
      (let [xpaths (u/expand-path ks-at-path* path)
            num-results (count xpaths)
            xpaths* (if wildcard?
                      [wildcard-parent]
                      xpaths)]
        (if (zero? num-results)
          [[] xpaths*]
          ;; Use loop to stay in go block
          (loop [out []
                 i 0]
            (let [path* (nth xpaths i)
                  ret (get-state-and-expanded-path state path* prefix)
                  new-out (conj out (first ret))
                  new-i (inc i)]
              (if (not= num-results new-i)
                (recur new-out new-i)
                [new-out xpaths*])))))

      (and terminal-kw? join?)
      (let [xpaths (u/expand-path ks-at-path* (butlast path))
            num-results (count xpaths)
            xpaths* (if wildcard?
                      [wildcard-parent]
                      xpaths)]
        (if (zero? num-results)
          [[] xpaths*]
          (let [results (loop [out [] ;; Use loop to stay in go block
                               i 0]
                          (let [path* (nth xpaths i)
                                ret (get-state-and-expanded-path
                                     state path* prefix)
                                new-out (conj out (first ret))
                                new-i (inc i)]
                            (if (not= num-results new-i)
                              (recur new-out new-i)
                              new-out)))
                v (case last-path-k
                    :vivo/keys (range (count results))
                    :vivo/count (count results)
                    :vivo/concat (apply concat results))]
            [v xpaths*]))))))

(defn <get-state-and-expanded-path [state path prefix]
  ;; TODO: Implement
  (au/go
    ))

(defn get-path-info [acc path db local-state *subject-id]
  (if (= [:vivo/subject-id] path)
    {:v @*subject-id}
    (let [path (resolve-symbols-in-path acc path)
          [head & tail] path
          state (case head
                  :local local-state
                  :sys db)]
      (u/sym-map state path head))))

(defn <get-subscription-state [ordered-pairs db local-state *subject-id]
  ;; This is async because it may need to fetch some state from either
  ;; local async storage or the server (in the future)

  ;; TODO: Store expanded paths - MUST DO BEFORE USING!!

  #_(au/go
      (let [num-pairs (count ordered-pairs)]
        (if (zero? num-pairs)
          {}
          ;; Use loop to stay in go block
          (loop [acc {}
                 i 0]
            (let [[sym path*] (nth ordered-pairs i)
                  info (get-path-info acc path* db local-state *subject-id)
                  {:keys [v state path head]} info
                  v* (or v
                         (-> (<get-state-and-expanded-path state path head)
                             (au/<?)
                             (first)))
                  new-acc (assoc acc sym v*)
                  new-i (inc i)]
              (if (= num-pairs new-i)
                new-acc
                (recur new-acc new-i))))))))

(defn in-db-cache? [path]
  ;; TODO: Expand when offline data is implemented
  true)

(defn cached? [ordered-pairs]
  (reduce (fn [acc [sym path]]
            (if (in-db-cache? path)
              acc
              (reduced false)))
          true ordered-pairs))

(defn get-synchronous-state-and-expanded-paths
  [ordered-pairs db local-state *subject-id]
  (reduce
   (fn [acc [sym path*]]
     (if-not (in-db-cache? path*)
       (reduced {:state :vivo/unknown})
       (let [info (get-path-info (:state acc) path* db local-state *subject-id)
             {:keys [v state path head]} info
             [v* xps] (get-state-and-expanded-path state path head)]
         (-> acc
             (update :state assoc sym (or v v*))
             (update :expanded-paths concat xps)))))
   {:state {}
    :expanded-paths []}
   ordered-pairs))

(defn make-applied-update-fn*
  [sub-name new-state expanded-paths *sub-name->info]
  (when-let [old-sub-info (@*sub-name->info sub-name)]
    (when (not= (:state old-sub-info) new-state)
      (fn []
        (let [{:keys [update-fn]} old-sub-info
              new-sub-info (-> old-sub-info
                               (assoc :state new-state)
                               (assoc :expanded-paths expanded-paths))]
          (swap! *sub-name->info assoc sub-name new-sub-info)
          (update-fn new-state))))))

(defn make-applied-update-fn
  [sub-name db local-state *sub-name->info *subject-id]
  (let [sub-info (@*sub-name->info sub-name)
        {:keys [ordered-pairs update-fn]} sub-info]
    (if (cached? ordered-pairs)
      (let [sxps (get-synchronous-state-and-expanded-paths
                  ordered-pairs db local-state *subject-id)
            {:keys [state expanded-paths]} sxps]
        (make-applied-update-fn* sub-name state expanded-paths *sub-name->info))
      #(ca/go
         (try
           ;; TODO: Implement. Follow synchronous patterns.
           #_(let [new-state (au/<? (<get-subscription-state
                                     ordered-pairs db local-state *subject-id))
                   f (make-applied-update-fn* sub-name new-state
                                              *sub-name->info)]
               (when f
                 (f)))
           (catch #?(:cljs js/Error :clj Exception) e
             (log/error (str "Error while updating `" sub-name "`:\n"
                             (u/ex-msg-and-stacktrace e)))))))))

(defn get-update-fn-info
  [sub-names db local-state *sub-name->info *subject-id]
  (reduce
   (fn [acc sub-name]
     (let [{:keys [react?]} (@*sub-name->info sub-name)
           update-fn* (make-applied-update-fn sub-name db local-state
                                              *sub-name->info
                                              *subject-id)]
       (cond
         (not update-fn*)
         acc

         react?
         (update acc :react-update-fns conj update-fn*)

         :else
         (update acc :non-react-update-fns conj update-fn*))))
   {:react-update-fns []
    :non-react-update-fns []}
   sub-names))

(defn update-subs!
  [sub-names db local-state *sub-name->info *subject-id]
  (let [update-fn-info (get-update-fn-info sub-names db local-state
                                           *sub-name->info *subject-id)
        {:keys [react-update-fns non-react-update-fns]} update-fn-info]
    (doseq [f non-react-update-fns]
      (f))
    (react/batch-updates
     #(doseq [rf react-update-fns]
        (rf)))))

(defn start-subscription-update-loop! [subs-update-ch]
  (ca/go-loop []
    (try
      (let [info (au/<? subs-update-ch)
            {:keys [db local-state update-infos cb
                    *sub-name->info *subject-id]} info]
        (-> (get-sub-names-to-update db local-state update-infos
                                     *sub-name->info)
            (order-by-lineage *sub-name->info)
            (update-subs! db local-state
                          *sub-name->info *subject-id))
        (when cb
          (cb true)))
      (catch #?(:cljs js/Error :clj Exception) e
        (log/error (str "Error while reading from subs-update-ch:\n"
                        (u/ex-msg-and-stacktrace e)))))
    (recur)))

(defn subscribe!
  [sub-name sub-map update-fn opts
   *stopped? *sub-name->info *sys-db-info *local-state *subject-id]
  (when-not @*stopped?
    (let [{:keys [react? resolution-map]} opts
          ordered-pairs (u/sub-map->ordered-pairs sub-map resolution-map)
          parents (set (:parents opts))
          db (:db @*sys-db-info)
          local-state @*local-state]
      (if (cached? ordered-pairs)
        (let [sxps (get-synchronous-state-and-expanded-paths
                    ordered-pairs db local-state *subject-id)
              {:keys [state expanded-paths]} sxps
              info (u/sym-map ordered-pairs resolution-map expanded-paths
                              parents react? update-fn state)]
          (swap! *sub-name->info assoc sub-name info)
          state)
        (do ;; TODO: Implement
          #_(let [f (make-applied-update-fn sub-name db local-state
                                            *sub-name->info *subject-id)]
              (f)
              :vivo/unknown)))))) ;; async update will happen later via update-fn