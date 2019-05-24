(ns com.dept24c.vivo.bristlecone-state-provider-impl
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.state :as state]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.lancaster :as l]))

(def default-bsp-opts
  {:log-error println
   :log-info println})

(defprotocol IBSPInternals
  (<handle-notify-subscriber [this arg metadata])
  (handle-request-pcf [this arg metadata])
  (shutdown [this]))

(defrecord BristleconeStateProvider
    [capsule-client state-schema log-error log-info *sub-id->sub *fp->pcf]

  state/IState
  (update-state! [this update-commands tx-info cb]
    (let [tx-info-str (u/edn->str tx-info)
          cmds (reduce
                (fn [acc [path [op arg]]]
                  (let [spath (u/path->spath path)
                        arg* (u/edn->value-rec state-schema path arg)]
                    (conj acc #:update-command{:path spath
                                               :op op
                                               :arg arg*})))
                [] update-commands)
          arg #:update-state-arg{:tx-info-str tx-info-str
                                 :update-commands cmds}
          success-cb (fn [ret]
                       (cb true))
          failure-cb (fn [e]
                       (cb false))]
      (cc/send-msg capsule-client :update-state arg success-cb failure-cb)
      nil))

  (subscribe! [this sub-id sub-map update-fn]
    (let [info (reduce-kv
                (fn [acc k path]
                  (-> acc
                      (assoc-in [:sub-map* (name k)] (u/path->spath path))
                      (assoc-in [:k->path k] path)))
                {:sub-map* {}
                 :k->path {}} sub-map)
          {:keys [sub-map* k->path]} info
          arg #:subscribe-arg{:sub-id sub-id
                              :sub-map sub-map*}
          sub (u/sym-map update-fn k->path)]
      (swap! *sub-id->sub assoc sub-id sub)
      (cc/send-msg capsule-client :subscribe arg)
      nil))

  (unsubscribe! [this sub-id]
    (let [success-cb #(log-info (str "Unsubscribe success: " %))
          failure-cb #(log-error (str "Unsubscribe failure: " %))]
      (swap! *sub-id->sub dissoc sub-id)
      (cc/send-msg capsule-client :unsubscribe sub-id success-cb failure-cb))
    nil)

  IBSPInternals
  (<handle-notify-subscriber [this arg metadata]
    (ca/go
      (try
        (let [{:notify-subscriber-arg/keys [sub-id data-frame tx-info-str]} arg
              {:keys [update-fn k->path]} (@*sub-id->sub sub-id)]
          (when update-fn
            (let [data-frame* (reduce-kv
                               (fn [acc k* v]
                                 (let [k (keyword k*)
                                       path (k->path k)]
                                   (assoc acc k (u/value-rec->edn
                                                 state-schema path v))))
                               {} data-frame)
                  tx-info (when tx-info-str
                            (u/str->edn tx-info-str))]
              (update-fn data-frame* tx-info))))
        (catch #?(:clj Exception :cljs js/Error) e
          (log-error (str "Error in <handle-notify-subscriber: "
                          (u/ex-msg-and-stacktrace e)))))))

  (handle-request-pcf [this fp metadata]
    (@*fp->pcf fp))

  (shutdown [this]
    (cc/shutdown capsule-client)))

(defn bristlecone-state-provider
  [get-server-url state-schema opts]
  (let [*sub-id->sub (atom {})
        *fp->pcf (atom (reduce (fn [acc sch]
                                 (assoc acc (l/fingerprint64 sch) (l/pcf sch)))
                               {} (l/sub-schemas state-schema)))
        {:keys [log-error log-info]} (merge default-bsp-opts opts)
        get-credentials (constantly {:subject-id "bristlecone-state-provider"
                                     :subject-secret ""})
        protocol (u/make-bsp-bs-protocol state-schema)
        capsule-client (cc/client get-server-url get-credentials
                                  protocol :state-provider)
        bsp (->BristleconeStateProvider capsule-client state-schema log-error
                                        log-info *sub-id->sub *fp->pcf)]
    (cc/set-handler capsule-client :notify-subscriber
                    (partial <handle-notify-subscriber bsp))
    (cc/set-handler capsule-client :request-pcf
                    (partial handle-request-pcf bsp))
    bsp))
