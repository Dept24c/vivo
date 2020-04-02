(ns user
  (:require
   [clojure.tools.namespace.repl :refer [refresh refresh-all]]
   [com.dept24c.vivo :as vivo]
   [com.dept24c.vivo.state-schema :as ss]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]))

(def default-server-port 12345)
(def repository-name "vivo-test")
(def stop-server nil)

(defn eq-or-parent? [parent-path test-path]
  (let [[relationship _] (u/relationship-info parent-path test-path)]
    (#{:equal :parent} relationship)))

(defn <authorized? [subject-id path read-write-or-call v]
  (au/go
    ;; Note that path includes the :sys prefix.
    (cond
      ;;(= [:sys :secret] path) false
      (eq-or-parent? [:sys :secret] path) false
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
                 :rpcs ss/rpcs
                 :state-schema ss/state-schema
                 ;; TODO: Fix or remove tx-fns
                 ;;:tx-fns tx-fns
                 }
         server (vivo/vivo-server config)]
     (vivo/set-rpc-handler! server :inc (fn [arg metadata]
                                          (inc arg)))
     (vivo/set-rpc-handler! server :authed/inc (fn [arg metadata]
                                                 (inc arg)))
     (alter-var-root
      #'stop-server
      (fn [_]
        (let [stopper #(vivo/shutdown-server! server)]
          (u/configure-capsule-logging :info)
          stopper)))
     nil)))

(defn restart []
  (stop)
  (refresh :after 'user/start))

(defn -main [& args]
  (let [[port-str] args
        port (if port-str
               (Integer/parseInt port-str)
               default-server-port)]
    (start port)))
