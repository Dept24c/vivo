(ns com.dept24c.vivo.client.subscriptions
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
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
            (if (#{:vivo/* :vivo/concat :vivo/keys :vivo/count} k)
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
  [db local-state update-infos *subscriber-name->info]
  (reduce-kv (fn [acc sub-name info]
               (let [{:keys [ordered-pairs]} info
                     paths (map second ordered-pairs)]
                 (if (update-sub? update-infos paths)
                   (conj acc sub-name)
                   acc)))
             #{} @*subscriber-name->info))

(defn order-by-lineage [sub-names-to-update *subscriber-name->info]
  (let [g (reduce
           (fn [acc sub-name]
             (let [{:keys [parents]} (@*subscriber-name->info sub-name)
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
    (filter #(not= :vivo/root %) (dep/topo-sort g))))

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
  (let [last-path-k (last path)
        join? (u/has-join? path)
        term-kw? (u/terminal-kw? last-path-k)
        ks-at-path* #(ks-at-path :vivo/* state % prefix path)]
    (cond
      (u/empty-sequence-in-path? path)
      [nil [path]]

      (and (not term-kw?) (not join?))
      (let [{:keys [norm-path val]} (commands/get-in-state state path prefix)]
        [val [norm-path]])

      (and term-kw? (not join?))
      (let [path* (butlast path)
            val (case last-path-k
                  :vivo/keys (ks-at-path :vivo/keys state path* prefix path)
                  :vivo/count (count-at-path state path* prefix)
                  :vivo/concat (do-concat state path* prefix))]
        [val [path]])

      (and (not term-kw?) join?)
      (let [xpath (u/expand-path ks-at-path* path)
            num-results (count xpath)]
        (if (zero? num-results)
          [[] []]
          ;; Use loop to stay in go block
          (loop [out []
                 i 0]
            (let [path* (nth xpath i)
                  ret (get-state-and-expanded-path state path* prefix)
                  new-out (conj out (first ret))
                  new-i (inc i)]
              (if (not= num-results new-i)
                (recur new-out new-i)
                [new-out xpath])))))

      (and term-kw? join?)
      (let [xpath (u/expand-path ks-at-path* (butlast path))
            num-results (count xpath)]
        (if (zero? num-results)
          [[] []]
          (let [results (loop [out [] ;; Use loop to stay in go block
                               i 0]
                          (let [path* (nth xpath i)
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
            [v xpath]))))))

(defn <get-state-and-expanded-path [state path prefix]
  ;; TODO: DRY this up with the sync version when implementing
  ;; offline/online data
  #_
  (au/go
    (let [last-path-k (last path)
          join? (u/has-join? path)
          term-kw? (u/terminal-kw? last-path-k)
          <ks-at-path* #(<ks-at-path :vivo/* state % prefix path)]
      (cond
        (u/empty-sequence-in-path? path)
        [nil [path]]

        (and (not term-kw?) (not join?))
        (let [{:keys [norm-path val]} (commands/get-in-state state path prefix)]
          [val [norm-path]])

        (and term-kw? (not join?))
        (let [path* (butlast path)
              val (case last-path-k
                    :vivo/keys (au/<? (<ks-at-path :vivo/keys state path* prefix
                                                   path))
                    :vivo/count (count-at-path state path* prefix)
                    :vivo/concat (do-concat state path* prefix))]
          [val [path]])

        (and (not term-kw?) join?)
        (let [xpath (au/<? (u/<expand-path <ks-at-path* path))
              num-results (count xpath)]
          (if (zero? num-results)
            [[] []]
            ;; Use loop to stay in go block
            (loop [out []
                   i 0]
              (let [path* (nth xpath i)
                    ret (au/<? (<get-state-and-expanded-path
                                state path* prefix))
                    new-out (conj out (first ret))
                    new-i (inc i)]
                (if (not= num-results new-i)
                  (recur new-out new-i)
                  [new-out xpath])))))

        (and term-kw? join?)
        (let [<ks-at-path* #(<ks-at-path :vivo/* state % prefix path)
              xpath (au/<? (u/<expand-path <ks-at-path* (butlast path)))
              num-results (count xpath)]
          (if (zero? num-results)
            [[] []]
            (let [results (loop [out [] ;; Use loop to stay in go block
                                 i 0]
                            (let [path* (nth xpath i)
                                  ret (au/<? (<get-state-and-expanded-path
                                              state path* prefix))
                                  new-out (conj out (first ret))
                                  new-i (inc i)]
                              (if (not= num-results new-i)
                                (recur new-out new-i)
                                new-out)))
                  v (case last-path-k
                      :vivo/keys (range (count results))
                      :vivo/count (count results)
                      :vivo/concat (apply concat results))]
              [v xpath])))))))

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
  (au/go
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

(defn get-synchronous-state [ordered-pairs db local-state *subject-id]
  (reduce
   (fn [acc [sym path*]]
     (if-not (in-db-cache? path*)
       (reduced :vivo/unknown)
       (let [info (get-path-info acc path* db local-state *subject-id)
             {:keys [v state path head]} info
             v* (or v (first (get-state-and-expanded-path state path head)))]
         (assoc acc sym v*))))
   {}
   ordered-pairs))

(defn do-update* [*subscriber-name->info sub-name new-state]
  ;; Get the info again to ensure it's still subscribed
  (when-let [info (@*subscriber-name->info sub-name)]
    (let [{:keys [*state update-fn]} info]
      (when-not (= @*state new-state)
        (reset! *state new-state)
        (update-fn new-state)))))

(defn do-async-update!
  [ordered-pairs db local-state sub-name log-error *subject-id
   *subscriber-name->info]
  (ca/go
    (try
      (let [new-state (au/<? (<get-subscription-state
                              ordered-pairs db local-state *subject-id))]
        (do-update* *subscriber-name->info sub-name new-state))
      (catch #?(:cljs js/Error :clj Exception) e
        (log-error (str "Error while updating `" sub-name "`:\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn update-sub!
  [sub-name db local-state log-error *subscriber-name->info *subject-id]
  (when-let [sub-info (@*subscriber-name->info sub-name)]
    (let [{:keys [ordered-pairs]} sub-info
          cached? (reduce (fn [acc [sym path]]
                            (if (in-db-cache? path)
                              acc
                              (reduced false)))
                          true ordered-pairs)]
      (if cached?
        (do-update* *subscriber-name->info sub-name
                    (get-synchronous-state ordered-pairs db local-state
                                           *subject-id))
        (do-async-update! ordered-pairs db local-state sub-name log-error
                          *subject-id *subscriber-name->info)))))

(defn update-subs!
  [sub-names db local-state log-error *subscriber-name->info *subject-id]
  (react/batch-updates
   #(doseq [sub-name sub-names]
      (update-sub! sub-name db local-state log-error *subscriber-name->info
                   *subject-id))))

(defn start-subscription-update-loop! [subs-update-ch log-error]
  (ca/go-loop []
    (try
      (let [info (au/<? subs-update-ch)
            {:keys [db local-state update-infos cb
                    *subscriber-name->info *subject-id]} info]
        (-> (get-sub-names-to-update db local-state update-infos
                                     *subscriber-name->info)
            (order-by-lineage *subscriber-name->info)
            (update-subs! db local-state log-error
                          *subscriber-name->info *subject-id))
        (when cb
          (cb true)))
      (catch #?(:cljs js/Error :clj Exception) e
        (log-error (str "Error while reading from subs-update-ch:\n"
                        (u/ex-msg-and-stacktrace e)))))
    (recur)))

(defn subscribe!
  [ordered-pairs initial-state update-fn subscriber-name parents* log-error
   *stopped? *subscriber-name->info *sys-db-info *local-state *subject-id]
  (when-not @*stopped?
    (when (contains? @*subscriber-name->info subscriber-name)
      (throw (ex-info (str "There is already a subscription named `"
                           subscriber-name
                           "`. Subscriber names must be unique.")
                      (u/sym-map subscriber-name ordered-pairs parents*))))
    (let [parents (set parents*)
          *state (atom initial-state)
          info (u/sym-map ordered-pairs parents update-fn *state)
          unsubscribe! (fn []
                         (swap! *subscriber-name->info dissoc subscriber-name))]
      (swap! *subscriber-name->info assoc subscriber-name info)
      (when (or (nil? initial-state)
                (= :vivo/unknown initial-state))
        (let [db (:db @*sys-db-info)
              local-state @*local-state]
          (update-sub! subscriber-name db local-state log-error
                       *subscriber-name->info *subject-id)))
      unsubscribe!)))
