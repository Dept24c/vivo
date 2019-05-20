(ns com.dept24c.vivo.bristlecone-server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [com.dept24c.vivo.mem-state-provider-impl :as mspi]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.endpoint :as ep]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.lancaster :as l])
  (:import
   (clojure.lang ExceptionInfo)))

(def default-opts
  {:log-info println
   :log-error println}) ;; TODO: use stderr

(defn <handle-update-state
  [ep state-schema *state *sub-id->sub *fp->pcf us-arg metadata]
  (let [sub-id->sub @*sub-id->sub]
    (au/go
      (let [{:update-state-arg/keys [tx-info-str update-commands]} us-arg
            <sender (partial ep/<send-msg ep (:conn-id metadata))
            <fp->wschema (u/make-<fp->wschema <sender *fp->pcf)
            orig-state @*state
            cmds (->> update-commands
                      (map-indexed (fn [i {:update-command/keys [path op arg]}]
                                     (au/go
                                       (let [path* (u/spath->path path)
                                             arg* (au/<? (u/<svalue->edn
                                                          <fp->wschema
                                                          state-schema
                                                          path* arg))
                                             upex (if (= :remove op)
                                                    [op]
                                                    [op arg*])]
                                         [i path* upex]))))
                      (ca/merge)
                      (ca/reduce (fn [acc ret]
                                   (if (instance? Throwable ret)
                                     (reduced ret)
                                     (let [[i p u] ret]
                                       (assoc acc i [p u]))))
                                 (sorted-map))
                      (au/<?)
                      (reduce (fn [acc [i cmd]]
                                (conj acc cmd))
                              []))
            new-state (reduce (fn [acc [path upex]]
                                (mspi/eval-upex acc path upex))
                              orig-state cmds)]
        (reset! *state new-state)
        (doseq [[sub-id {:keys [sub-map update-fn]}] sub-id->sub]
          (when (reduce (fn [acc sub-path]
                          (let [orig-v (u/get-in-state orig-state sub-path)
                                new-v (u/get-in-state new-state sub-path)]
                            (if (= orig-v new-v)
                              acc
                              (reduced true))))
                        false (vals sub-map))
            (update-fn (u/make-data-frame sub-map new-state) tx-info-str)))
        true))))

(defn data-frame->sdata-frame [k->path state-schema data-frame]
  (reduce-kv (fn [acc k v]
               (try
                 (let [path (k->path k)
                       svalue (u/edn->svalue state-schema path v)]
                   (assoc acc k svalue))
                 (catch ExceptionInfo e
                   (if (str/includes? (u/ex-msg e)
                                      "Data `nil` (type: nil) is not a valid")
                     (reduced nil) ;; A value is nil that can't be...
                     (throw e)))))
             {} data-frame))

(defn handle-subscribe
  [ep state-schema *state *sub-id->sub *conn-id->sub-ids arg metadata]
  (let [{:subscribe-arg/keys [sub-map sub-id]} arg
        {:keys [conn-id]} metadata
        info (reduce-kv (fn [acc k spath]
                          (let [path (u/spath->path spath)]
                            (-> acc
                                (assoc-in [:sub-map* (name k)] path)
                                (assoc-in [:k->path k] path))))
                        {:sub-map* {}
                         :k->path {}} sub-map)
        {:keys [sub-map* k->path]} info
        update-fn (fn [data-frame tx-info-str]
                    (let [sdf (data-frame->sdata-frame k->path state-schema
                                                       data-frame)
                          arg #:notify-subscriber-arg{:sub-id sub-id
                                                      :data-frame sdf
                                                      :tx-info-str tx-info-str}]
                      (if sdf
                        (ep/send-msg ep conn-id :notify-subscriber arg)
                        (do
                          ;; TODO: Log a warning here that update will not be
                          ;; sent since something is nil that can't be
                          ))))
        sub {:conn-id conn-id
             :sub-map sub-map*
             :update-fn update-fn}
        state @*state]
    (swap! *sub-id->sub assoc sub-id sub)
    (swap! *conn-id->sub-ids update conn-id conj sub-id)
    (update-fn (u/make-data-frame sub-map* state) nil)
    true))

(defn handle-unsubscribe [ep *sub-id->sub *conn-id->sub-ids sub-id metadata]
  (let [{:keys [conn-id]} (@*sub-id->sub sub-id)]
    (swap! *conn-id->sub-ids update conn-id disj sub-id)
    (swap! *sub-id->sub dissoc sub-id))
  true)

(defn handle-request-pcf [ep *fp->pcf fp metadata]
  (@*fp->pcf fp))

(defn bristlecone-server
  ([port state-schema]
   (bristlecone-server port state-schema {}))
  ([port state-schema opts]
   (let [{:keys [log-info log-error]} (merge default-opts opts)
         *state (atom nil)
         *sub-id->sub (atom {})
         *conn-id->sub-ids (atom {})
         *fp->pcf (atom (reduce (fn [acc sch]
                                  (assoc acc (l/fingerprint64 sch) (l/pcf sch)))
                                {} (l/sub-schemas state-schema)))
         on-client-disconnect (fn [{:keys [conn-id]}]
                                (let [sub-ids (@*conn-id->sub-ids conn-id)]
                                  (swap! *sub-id->sub #(apply dissoc % sub-ids))
                                  (swap! *conn-id->sub-ids dissoc conn-id)))
         ep (ep/endpoint "bsp" (constantly true) u/bsp-bs-protocol :server
                         {:on-disconnect on-client-disconnect})
         capsule-server (cs/server [ep] port {})]
     (ep/set-handler ep :update-state
                     (partial <handle-update-state ep state-schema
                              *state *sub-id->sub *fp->pcf))
     (ep/set-handler ep :subscribe
                     (partial handle-subscribe ep state-schema
                              *state *sub-id->sub *conn-id->sub-ids))
     (ep/set-handler ep :unsubscribe
                     (partial handle-unsubscribe ep *sub-id->sub))
     (ep/set-handler ep :request-pcf (partial handle-request-pcf ep *fp->pcf))
     (cs/start capsule-server)
     (log-info (str "Bristlecone server started on port " port "."))
     #(cs/stop capsule-server))))
