(ns user
  (:require
   [clojure.tools.namespace.repl :refer [refresh refresh-all]]
   [cognitect.aws.client.api :as aws]
   [com.dept24c.vivo.bristlecone.ddb-block-storage :as ddb]
   [com.dept24c.vivo.server :as server]
   [com.dept24c.vivo.state-schema :as ss]
   [com.dept24c.vivo.test-user :as tu]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [puget.printer :refer [cprint]]))

(def default-server-port 12345)
(def repository-name "vivo-test")
(def stop-server nil)

(defn <authorized? [subject-id path read-write-or-call v]
  (au/go
    ;; Note that path includes the :sys prefix.
    (cond
      (= [:sys :secret] path) false
      (= [:vivo/rpcs :authed/inc] path) (boolean subject-id)
      :else true)))

(defn stop []
  (if stop-server
    (do
      (println "Stopping server...")
      (stop-server))
    (println "Server is not running.")))

(defn make-user-id-to-msgs [{:syms [msgs]}]
  (reduce (fn [acc {:keys [user-id] :as msg}]
            (update acc user-id conj msg))
          {} msgs))

(defn start
  ([]
   (start default-server-port))
  ([port]
   (let [tx-fns [{:name "Make user-id to msgs index"
                  :sub-map '{msgs [:sys :msgs]}
                  :f make-user-id-to-msgs
                  :output-path [:sys :user-id-to-msgs]}]
         config {:authenticate-admin-client (constantly true)
                 :authorization-fn <authorized?
                 :port port
                 :repository-name repository-name
                 :rpc-name-kw->info ss/rpc-name-kw->info
                 :state-schema ss/state-schema
                 :tx-fns tx-fns}]
     (alter-var-root
      #'stop-server
      (fn [_]
        (let [stopper (server/vivo-server config)]
          (u/configure-capsule-logging :info)
          stopper))))))

;; Note: This has problems due to not having socket address reuse
(defn restart []
  (stop)
  (refresh :after 'user/start))

(defn -main [& args]
  (let [[port-str] args
        port (if port-str
               (Integer/parseInt port-str)
               default-server-port)]
    (start port)))
