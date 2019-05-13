(ns user
  (:require
   [clojure.tools.namespace.repl :refer [refresh refresh-all]]
   [com.dept24c.vivo.bristlecone-server :as bs]
   [com.dept24c.vivo.state-schema :as ss]
   [deercreeklabs.capsule.logging :as logging]
   [puget.printer :refer [cprint]]))

(def default-server-port 12345)

(def *system (atom nil))

(defn configure-logging []
  (logging/add-log-reporter! :println logging/println-reporter)
  (logging/set-log-level! :debug))

(defn stop []
  (when-let [stopper @*system]
    (println "Stopping server...")
    (stopper)))

(defn start
  ([]
   (start default-server-port))
  ([port]
   (configure-logging)
   (reset! *system (bs/bristlecone-server port ss/state-schema))))

(defn restart []
  (stop)
  (refresh :after 'user/start))

(defn -main [& args]
  (let [[port] args]
    (start (or port default-server-port))))
