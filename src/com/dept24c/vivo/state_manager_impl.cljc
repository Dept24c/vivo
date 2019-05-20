(ns com.dept24c.vivo.state-manager-impl
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [com.dept24c.vivo.state :as state]
   [com.dept24c.vivo.utils :as u]
   [rum.core :as rum]
   [weavejester.dependency :as dep])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(defn make-df-key->depend-info [sub-map]
  (try
    (let [g (reduce-kv (fn [acc sym v]
                         (if (= :vivo/tx-info v)
                           acc
                           (if-let [deps (filter symbol? v)]
                             (reduce (fn [acc* dep]
                                       (dep/depend acc* sym dep))
                                     acc deps)
                             acc)))
                       (dep/graph) sub-map)
          keywordize #(set (map keyword %))]
      (reduce (fn [acc sym]
                (assoc acc (keyword sym)
                       {:dependents
                        (keywordize (dep/immediate-dependents g sym))
                        :dependencies
                        (keywordize (dep/immediate-dependencies g sym))}))
              {} (keys sub-map)))
    (catch #?(:clj ExceptionInfo :cljs js/Error) e
      (if (str/includes? (u/ex-msg e) "Circular dependency")
        (let [{:keys [node dependency]} (ex-data e)]
          (throw
           (ex-info (str "Circular dependency in subscription map. " node
                         " and " dependency " are mutually dependent. ")
                    {:mutually-dependent-symbols [node dependency]})))
        (throw e)))))

(defn resolve-path [path data-frame]
  (map #(if (symbol? %)
          (data-frame (keyword %))
          %)
       path))

(defn update-data-frame!
  [*data-frame tx-info df-updates df-key->info *different?]
  (let [old-df @*data-frame
        already-different? @*different?
        info (reduce-kv
              (fn [acc df-key new-v]
                (let [old-v (old-df df-key)
                      diff? (and (not (:different? acc))
                                 (not= old-v new-v))]
                  (cond-> (assoc-in acc [:new-df df-key] new-v)
                    diff? (assoc :different? true))))
              {:new-df old-df
               :different? already-different?}
              df-updates)
        {:keys [new-df different?]} info]
    (when (and (not already-different?)
               different?)
      (reset! *different? true))
    (reset! *data-frame (cond-> new-df
                          tx-info (assoc (df-key->info :vivo/tx-info)
                                         tx-info)))))

(defn handle-updates! [sub df-updates tx-info]
  (let [{:keys [sub-id sp-update-fn update-fn root-df-keys
                df-key->info *data-frame *prior-data-frame
                *different? *df-keys-needing-update]} sub
        data-frame (update-data-frame! *data-frame tx-info df-updates
                                       df-key->info *different?)
        update-ks (set (keys df-updates))
        dependent-ks (reduce (fn [acc df-key]
                               (set/union acc (-> (df-key->info df-key)
                                                  (:dependents))))
                             #{} update-ks)
        df-keys-needing-update (swap! *df-keys-needing-update
                                      #(-> (set/difference % update-ks)
                                           (set/union dependent-ks)))
        sp->sub-map (reduce
                     (fn [acc df-key]
                       (let [{:keys [sp abstract-path]} (df-key->info df-key)
                             path (resolve-path abstract-path data-frame)]
                         (assoc-in acc [sp df-key] path)))
                     {} (if (empty? update-ks)
                          root-df-keys
                          dependent-ks))]
    (if (empty? sp->sub-map)
      (when (and (empty? df-keys-needing-update)
                 @*different?)
        (reset! *prior-data-frame data-frame)
        (update-fn data-frame))
      (doseq [[sp sub-map] sp->sub-map]
        (state/subscribe! sp sub-id sub-map sp-update-fn)))))

(defn throw-invalid-root [path]
  (let [[path-root & rest-path] path]
    (throw (ex-info (str "No state provider exists for root `" path-root
                         "`. Path: `" path "`.")
                    (u/sym-map path-root path)))))

(defrecord StateManager [root->sp *sub-id->sub]
  state/IState
  (update-state! [this update-commands tx-info cb]
    (when-not (seqable? update-commands)
      (throw (ex-info "The update-commands parameter must be a seqable"
                      (u/sym-map update-commands))))
    (let [;;  Use reduce, not reduce-kv, to enable ordered seqs
          sp->um (cond-> (reduce
                          (fn [acc [path upex]]
                            (when-not (sequential? path)
                              (throw
                               (ex-info (str "Invalid path in update-commands."
                                             " Must be a sequence.")
                                        {:path path})))
                            (let [[path-root & rest-path] path
                                  sp (root->sp path-root)]
                              (when-not sp
                                (throw-invalid-root path))
                              (update acc sp #(conj (or % [])
                                                    [rest-path upex]))))
                          {} update-commands))
          *sps-left (atom (count sp->um))
          check-tx-done (fn [ret]
                          (when (and cb (zero? (swap! *sps-left
                                                      (fn [sps-left]
                                                        (when sps-left
                                                          (if-not ret
                                                            nil
                                                            (dec sps-left)))))))
                            (cb true)))]
      (doseq [[sp sp-update-commands] sp->um]
        (state/update-state! sp sp-update-commands tx-info check-tx-done))))

  (subscribe! [this sub-id sub-map update-fn]
    (when-not (string? sub-id)
      (throw (ex-info "The sub-id parameter must be a string."
                      (u/sym-map sub-id))))
    (when-not (map? sub-map)
      (throw (ex-info "The sub-map parameter must be a map."
                      (u/sym-map sub-map))))
    (when-not (ifn? update-fn)
      (throw (ex-info "The update-fn parameter must be a function."
                      (u/sym-map update-fn))))
    (let [df-key->depend-info (make-df-key->depend-info sub-map)
          df-key->info (reduce-kv
                        (fn [acc df-sym v]
                          (if (= :vivo/tx-info v)
                            (assoc acc v (keyword df-sym))
                            (let [[root & abstract-path] v
                                  df-key (keyword df-sym)
                                  sp (root->sp root)
                                  _ (when (nil? sp)
                                      (throw-invalid-root v))
                                  info (df-key->depend-info df-key)
                                  {:keys [dependents dependencies]} info]
                              (assoc acc df-key
                                     (u/sym-map sp abstract-path dependents
                                                dependencies)))))
                        {} sub-map)
          sp-update-fn (fn [df-updates tx-info]
                         (when-let [sub (@*sub-id->sub sub-id)]
                           (handle-updates! sub df-updates tx-info)))
          root? #(-> (df-key->info %)
                     (:dependencies)
                     (empty?))
          df-keys (reduce-kv (fn [acc df-key v]
                               (if (= :vivo/tx-info v)
                                 acc
                                 (conj acc (keyword df-key))))
                             [] sub-map)
          root-df-keys (set (filter root? df-keys))
          *data-frame (atom {})
          *prior-data-frame (atom {})
          *different? (atom false)
          *df-keys-needing-update (atom (set df-keys))
          sub (u/sym-map sub-id sp-update-fn update-fn root-df-keys
                         df-key->info *data-frame *prior-data-frame
                         *different? *df-keys-needing-update)]
      (swap! *sub-id->sub assoc sub-id sub)
      (handle-updates! sub {} nil)))

  (unsubscribe! [this sub-id]
    (when-not (string? sub-id)
      (throw (ex-info "The sub-id parameter must be a string."
                      (u/sym-map sub-id))))
    (swap! *sub-id->sub dissoc sub-id)))

(defn check-root->sp [root->sp]
  (when-not (map? root->sp)
    (throw (ex-info (str "root->sp parameter must be a map of key path roots "
                         "(keywords) to state providers.")
                    (u/sym-map root->sp))))
  (doseq [[root sp] root->sp]
    (when-not (keyword? root)
      (throw (ex-info
              (str "root->sp parameter must be a map of key path roots "
                   "(keywords) to state providers. `" root "` is not a keyword.")
              (u/sym-map root root->sp))))
    (when-not (satisfies? state/IState sp)
      (throw (ex-info
              (str "root->sp parameter must be a map of key path roots "
                   "to state providers. `" sp "` does not satisfy the "
                   "IState protocol")
              (u/sym-map root sp root->sp))))))

(defn state-manager
  [root->sp]
  (check-root->sp root->sp)
  (when-not (map? root->sp)
    (throw (ex-info (str "The root->sp parameter must be a map. Got: " root->sp)
                    (u/sym-map root->sp))))
  (when (empty? root->sp)
    (throw (ex-info "The root->sp map must have at least one entry."
                    (u/sym-map root->sp))))
  (let [*sub-id->sub (atom {})]
    (->StateManager root->sp *sub-id->sub)))
