(ns com.dept24c.vivo.client.state-subscriptions
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
  (reduce (fn [acc {:keys [norm-path op]}]
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
     (if (update-sub?* update-infos (transform-operators-in-sub-path sub-path))
       (reduced true)
       false))
   false
   sub-paths))

(defn get-state-sub-names-to-update
  [update-infos *state-sub-name->info]
  (reduce-kv (fn [acc state-sub-name info]
               (let [{:keys [expanded-paths]} info]
                 (if (update-sub? update-infos expanded-paths)
                   (conj acc state-sub-name)
                   acc)))
             #{} @*state-sub-name->info))

(defn order-by-lineage [state-sub-names-to-update *state-sub-name->info]
  (let [g (reduce
           (fn [acc state-sub-name]
             (let [{:keys [parents]} (@*state-sub-name->info state-sub-name)
                   update-parents (set/intersection parents
                                                    state-sub-names-to-update)]
               (if (empty? update-parents)
                 (dep/depend acc state-sub-name :vivo/root)
                 (reduce (fn [acc* parent]
                           (if (state-sub-names-to-update parent)
                             (dep/depend acc* state-sub-name parent)
                             acc*))
                         acc
                         update-parents))))
           (dep/graph)
           state-sub-names-to-update)]
    (->> (dep/topo-sort g)
         (filter #(not= :vivo/root %)))))

(defn resolve-symbols-in-path [state path]
  (let [reducer (fn [acc element]
                  (conj acc (if-not (symbol? element)
                              element
                              (get state element))))]
    (if(symbol? path)
      (get state path)
      (reduce reducer [] path))))

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
    (when (and (not (nil? seqs))
               (or (not (sequential? seqs))
                   (not (sequential? (first seqs)))))
      (throw
       (ex-info
        (str "`:vivo/concat` terminates path, but there "
             "is not a sequence of sequences at " path ".")
        {:path path
         :value seqs})))
    (apply concat seqs)))

(defn get-value-and-expanded-paths [state path prefix subject-id]
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

      (= [:vivo/subject-id] path)
      [subject-id [path]]

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
                  ret (get-value-and-expanded-paths
                       state path* prefix subject-id)
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
                                ret (get-value-and-expanded-paths
                                     state path* prefix subject-id)
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

(defn <get-state-and-expanded-paths [state path prefix]
  ;; TODO: Implement
  (au/go
    ))

(defn get-path-info [acc-state path db local-state resolve-path?]
  (let [resolved-path (if resolve-path?
                        (resolve-symbols-in-path acc-state path)
                        path)
        [head & tail] resolved-path
        state-src (case head
                    :local local-state
                    :sys db
                    {})]
    (u/sym-map state-src resolved-path head)))

(defn in-db-cache? [path]
  ;; TODO: Expand when offline data is implemented
  true)

(defn cached? [pairs]
  (reduce (fn [acc [sym path]]
            (if (in-db-cache? path)
              acc
              (reduced false)))
          true pairs))

(defn get-synchronous-state-and-expanded-paths
  [independent-pairs ordered-dependent-pairs db local-state subject-id]
  (let [reducer* (fn [resolve-path? acc [sym path]]
                   (if-not (in-db-cache? path)
                     (reduced {:state :vivo/unknown})
                     (let [info (get-path-info (:state acc) path db local-state
                                               resolve-path?)
                           {:keys [state-src resolved-path head]} info
                           [v xps] (get-value-and-expanded-paths
                                    state-src resolved-path head subject-id)]
                       (-> acc
                           (update :state assoc sym v)
                           (update :expanded-paths concat xps)))))
        init {:state {}
              :expanded-paths []}
        indep-ret (reduce (partial reducer* false) init independent-pairs)]
    (reduce (partial reducer* true) indep-ret ordered-dependent-pairs)))

(defn make-applied-update-fn*
  [state-sub-name new-state expanded-paths *state-sub-name->info]
  (when-let [old-sub-info (@*state-sub-name->info state-sub-name)]
    (when (not= (:state old-sub-info) new-state)
      (fn []
        (let [{:keys [update-fn]} old-sub-info
              new-sub-info (-> old-sub-info
                               (assoc :state new-state)
                               (assoc :expanded-paths expanded-paths))]
          (swap! *state-sub-name->info assoc state-sub-name new-sub-info)
          (update-fn new-state))))))

(defn make-applied-update-fn
  [state-sub-name db local-state *state-sub-name->info subject-id]
  (let [sub-info (@*state-sub-name->info state-sub-name)
        {:keys [independent-pairs ordered-dependent-pairs update-fn]} sub-info]
    (if (and (cached? independent-pairs)
             (cached? ordered-dependent-pairs))
      (let [sxps (get-synchronous-state-and-expanded-paths
                  independent-pairs ordered-dependent-pairs db local-state
                  subject-id)
            {:keys [state expanded-paths]} sxps]
        (make-applied-update-fn* state-sub-name state expanded-paths
                                 *state-sub-name->info))
      #(ca/go
         (try
           ;; TODO: Implement. Follow synchronous patterns.

           (catch #?(:cljs js/Error :clj Exception) e
             (log/error (str "Error while updating `" state-sub-name "`:\n"
                             (u/ex-msg-and-stacktrace e)))))))))

(defn get-update-fn-info
  [state-sub-names db local-state *state-sub-name->info subject-id]
  (reduce
   (fn [acc state-sub-name]
     (let [{:keys [react?]} (@*state-sub-name->info state-sub-name)
           update-fn* (make-applied-update-fn state-sub-name db local-state
                                              *state-sub-name->info
                                              subject-id)]
       (cond
         (not update-fn*)
         acc

         react?
         (update acc :react-update-fns conj update-fn*)

         :else
         (update acc :non-react-update-fns conj update-fn*))))
   {:react-update-fns []
    :non-react-update-fns []}
   state-sub-names))

(defn update-subs!
  [state-sub-names db local-state *state-sub-name->info subject-id]
  (let [update-fn-info (get-update-fn-info state-sub-names db local-state
                                           *state-sub-name->info subject-id)
        {:keys [react-update-fns non-react-update-fns]} update-fn-info]
    (doseq [f non-react-update-fns]
      (f))
    (react/batch-updates
     #(doseq [rf react-update-fns]
        (rf)))))

(defn start-subscription-update-loop! [subscription-state-update-ch]
  (ca/go-loop []
    (try
      (let [info (au/<? subscription-state-update-ch)
            {:keys [db local-state subject-id update-infos
                    cb *state-sub-name->info]} info]
        (-> (get-state-sub-names-to-update update-infos *state-sub-name->info)
            (order-by-lineage *state-sub-name->info)
            (update-subs! db local-state *state-sub-name->info subject-id))
        (when cb
          (cb true)))
      (catch #?(:cljs js/Error :clj Exception) e
        (log/error (str "Error in subscription-update-loop."
                        (u/ex-msg-and-stacktrace e)))))
    (recur)))

(defn subscribe-to-state!
  [state-sub-name sub-map update-fn opts sys-db-info local-state subject-id
   *stopped? *state-sub-name->info]
  (when-not (string? state-sub-name)
    (throw (ex-info
            (str "The `state-sub-name` argument to `subscribe!` "
                 " must be a string. Got `" state-sub-name "`.")
            (u/sym-map state-sub-name sub-map opts))))
  (when-not @*stopped?
    (let [{:keys [react? resolution-map]} opts
          map-info (u/sub-map->map-info sub-map resolution-map)
          {:keys [independent-pairs ordered-dependent-pairs]} map-info
          parents (set (:parents opts))
          db (:db sys-db-info)]
      (if (and (cached? independent-pairs)
               (cached? ordered-dependent-pairs))
        (let [sxps (get-synchronous-state-and-expanded-paths
                    independent-pairs ordered-dependent-pairs db local-state
                    subject-id)
              {:keys [expanded-paths]} sxps
              state (select-keys (:state sxps) (keys sub-map))
              info (u/sym-map independent-pairs ordered-dependent-pairs
                              expanded-paths parents react? update-fn state)]
          (swap! *state-sub-name->info assoc state-sub-name info)
          state)
        (do ;; TODO: Implement
          #_(let [f (make-applied-update-fn state-sub-name db local-state
                                            *state-sub-name->info subject-id)]
              (f)
              :vivo/unknown)))))) ;; async update will happen later via update-fn
