(ns com.dept24c.vivo.server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [com.dept24c.vivo.bristlecone :as br]
   [com.dept24c.vivo.state :as state]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.endpoint :as ep]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.lancaster :as l])
  (:import
   (clojure.lang ExceptionInfo)))

(defn default-health-http-handler [req]
  (if (= "/health" (:uri req))
    {:status 200
     :headers {"content-type" "text/plain"
               "Access-Control-Allow-Origin" "*"}
     :body "I am healthy"}
    {:status 404
     :body "I still haven't found what you're looking for..."}))

(def default-opts
  {:authentication-fn (fn [identifier secret]
                        nil) ;; return subject-id or nil
   :authorization-fn (fn [subject-id path]
                       false) ;; return true or false
   :handle-http default-health-http-handler
   :http-timeout-ms 60000
   :log-info println
   :log-error println}) ;; TODO: use stderr

(defn make-tf-df [tf tx-info-str state]
  (let [{:keys [ordered-sym-path-pairs tx-info-sym]} tf
        df* (cond-> {}
              tx-info-sym (assoc tx-info-sym (u/str->edn tx-info-str)))
        last-i (dec (count ordered-sym-path-pairs))]
    (if-not (seq ordered-sym-path-pairs)
      df*
      (loop [df df*
             i 0]
        (let [[sym path] (nth ordered-sym-path-pairs i)
              [head & tail] (reduce (fn [acc k]
                                      (if-not (symbol? k)
                                        (conj acc k)
                                        (if-let [v (df k)]
                                          (conj acc v)
                                          (reduced nil))))
                                    [] path)
              v (:val (state/get-in-state state tail))
              new-df (assoc df sym v)]
          (if (= last-i i)
            new-df
            (recur new-df (inc i))))))))

(defn eval-tfs [tfs tx-info-str old-state new-state]
  (let [ret (reduce (fn [acc tf]
                      (let [old-df (make-tf-df tf tx-info-str old-state)
                            new-df (make-tf-df tf tx-info-str new-state)
                            {:keys [f output-path]} tf]
                        (if (= old-df new-df)
                          acc
                          (assoc-in acc (rest output-path) (f new-df)))))
                    new-state tfs)]
    ret))

(defn start-state-update-loop
  [ep ch *db-id-num *db-id->state authorization-fn tfs log-error]
  (ca/go-loop []
    (try
      (let [update (au/<? ch)
            {:keys [subject-id update-cmds tx-info-str conn-ids ret-ch]} update
            old-db-id (u/long->b62 @*db-id-num)
            db-id (u/long->b62 (swap! *db-id-num inc))
            old-state (@*db-id->state old-db-id)
            new-state (->> (reduce (fn [acc {:keys [path] :as cmd}]
                                     (if (authorization-fn subject-id path)
                                       (state/eval-cmd acc cmd)
                                       (reduced :vivo/unauthorized)))
                                   old-state update-cmds)
                           (eval-tfs tfs tx-info-str old-state))
            ret (if (= :vivo/unauthorized new-state)
                  false
                  (let [change-info (u/sym-map db-id tx-info-str)]
                    (swap! *db-id->state assoc db-id new-state)
                    (doseq [conn-id conn-ids]
                      (ep/send-msg ep conn-id :sys-state-changed change-info))
                    true))]
        (ca/>! ret-ch ret))
      (catch Exception e
        (log-error (str "Error in state-update-loop:\n"
                        (u/ex-msg-and-stacktrace e)))))
    (recur)))

(defn handle-set-branch [*conn-id->info *branch->info branch metadata]
  (let [{:keys [conn-id]} metadata]
    (swap! *conn-id->info update conn-id assoc :branch branch)
    (swap! *branch->info update branch
           (fn [{:keys [conn-ids] :as info}]
             (if conn-ids
               (update info :conn-ids conj conn-id)
               (assoc info :conn-ids #{conn-id}))))
    true))

(defn <handle-get-state
  [state-schema authorization-fn *conn-id->info *db-id->state *db-id-num
   arg metadata]
  (au/go
    (let [{:keys [path db-id]} arg
          {:keys [conn-id]} metadata
          {:keys [subject-id]} (@*conn-id->info conn-id)]
      (if-not (authorization-fn subject-id path)
        {:db-id db-id
         :is-unauthorized true}
        (let [db-id* (or db-id
                         ;; TODO: Get the latest db-id from the selected branch
                         (u/long->b62 @*db-id-num))
              state (@*db-id->state db-id*)
              {:keys [val]} (state/get-in-state state path)
              value (u/edn->value-rec state-schema path val)]
          {:db-id db-id*
           :value value})))))

(defn <handle-update-state
  [ep state-schema authorization-fn log-error tfs *conn-id->info
   *branch->info *db-id->state *db-id-num arg metadata]
  (au/go
    (let [{:keys [tx-info-str]} arg
          {:keys [conn-id]} metadata
          {:keys [subject-id branch]} (@*conn-id->info conn-id)
          xf-cmd (fn [{:keys [path] :as cmd}]
                   (update cmd :arg #(u/value-rec->edn state-schema path %)))
          update-cmds (map xf-cmd (:update-cmds arg))
          {:keys [update-ch conn-ids]}  (@*branch->info branch)
          ret-ch (ca/chan)
          ch (or update-ch
                 (let [ch* (ca/chan 100)]
                   (swap! *branch->info assoc-in [branch :update-ch] ch*)
                   (start-state-update-loop ep ch* *db-id-num *db-id->state
                                            authorization-fn tfs log-error)
                   ch*))]
      (ca/put! ch (u/sym-map subject-id update-cmds tx-info-str
                             conn-ids ret-ch))
      (au/<? ret-ch))))

(defn <handle-log-in [ep authentication-fn *conn-id->info arg metadata]
  (au/go
    (let [{:keys [identifier secret]} arg
          {:keys [conn-id]} metadata
          subject-id (authentication-fn identifier secret)]
      (when subject-id
        (swap! *conn-id->info update conn-id assoc :subject-id subject-id)
        (ep/send-msg ep conn-id :subject-id-changed subject-id))
      (boolean subject-id))))

(defn <handle-log-out [ep *conn-id->info arg metadata]
  (au/go
    (let [{:keys [conn-id]} metadata]
      (swap! *conn-id->info update conn-id dissoc :subject-id)
      (ep/send-msg ep conn-id :subject-id-changed nil)
      true)))

(defn xf-tfs [tfs]
  (reduce
   (fn [acc tf]
     (let [{:keys [sub-map f output-path]} tf]
       (when-not sub-map
         (throw (ex-info "Transaction function must have a :sub-map."
                         {:sub-map sub-map
                          :output-path output-path})))
       (when-not (map? sub-map)
         (throw (ex-info "Transaction function's :sub-map must be a map."
                         {:sub-map sub-map
                          :output-path output-path})))
       (let [undefined-syms (u/get-undefined-syms sub-map)]
         (when (seq undefined-syms)
           (throw
            (ex-info
             (str "Undefined symbol(s) in subscription map for transaction  "
                  "function. These symbols are used in subscription "
                  "keypaths, but are not defined: " undefined-syms)
             (u/sym-map undefined-syms sub-map output-path)))))
       (doseq [[sym path] sub-map]
         (when-not (= :sys (first path))
           (throw
            (ex-info (str "Paths in the :sub-map of a transaction function "
                          "must begin with `:sys`.")
                     {:sub-map sub-map
                      :output-path output-path}))))
       (when-not (pos? (count sub-map))
         (throw (ex-info
                 (str "Transaction function's :sub-map must contain at "
                      "least one entry.")
                 {:sub-map sub-map
                  :output-path output-path})))
       (when-not f
         (throw (ex-info "Transaction function must have an :f key and value."
                         {:sub-map sub-map
                          :output-path output-path})))
       (when-not output-path
         (throw (ex-info "Transaction function must have an :output-path."
                         {:sub-map sub-map
                          :output-path output-path})))
       (when-not (= :sys (first output-path))
         (throw (ex-info (str "Transaction function's :output-path must "
                              "begin with `:sys`.")
                         {:sub-map sub-map
                          :output-path output-path})))
       (let [info (state/make-sub-info sub-map)
             {:keys [ordered-sym-path-pairs tx-info-sym]} info
             tf (u/sym-map ordered-sym-path-pairs tx-info-sym f output-path)]
         (conj acc tf))))
   [] tfs))

(defn vivo-server
  ([port store-name state-schema]
   (vivo-server port store-name state-schema {}))
  ([port store-name state-schema opts]
   (let [{:keys [log-info log-error initial-sys-state
                 transaction-fns handle-http http-timeout-ms
                 authentication-fn authorization-fn]} (merge default-opts opts)
         protocol (u/make-sm-server-protocol state-schema)
         authenticator (constantly true)
         ep (ep/endpoint "state-manager" authenticator protocol :server)
         cs-opts (u/sym-map handle-http http-timeout-ms)
         capsule-server (cs/server [ep] port cs-opts)
         br-client (br/bristlecone-client store-name state-schema)
         transaction-fns* (xf-tfs transaction-fns)
         *conn-id->info (atom {})
         initial-db-id-num 0
         *db-id-num (atom initial-db-id-num)
         db-id (u/long->b62 initial-db-id-num)
         *db-id->state (atom {db-id initial-sys-state})
         *branch->info (atom {})]

     (ep/set-handler ep :log-in
                     (partial <handle-log-in ep authentication-fn
                              *conn-id->info))
     (ep/set-handler ep :log-out
                     (partial <handle-log-out ep *conn-id->info))
     (ep/set-handler ep :set-branch
                     (partial handle-set-branch
                              *conn-id->info *branch->info))
     (ep/set-handler ep :get-state
                     (partial <handle-get-state state-schema authorization-fn
                              *conn-id->info *db-id->state *db-id-num))
     (ep/set-handler ep :update-state
                     (partial <handle-update-state ep state-schema
                              authorization-fn log-error transaction-fns*
                              *conn-id->info *branch->info
                              *db-id->state *db-id-num))
     (cs/start capsule-server)
     (log-info (str "Vivo server started on port " port "."))
     #(cs/stop capsule-server))))
