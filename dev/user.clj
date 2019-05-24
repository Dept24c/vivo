(ns user
  (:require
   [clojure.tools.namespace.repl :refer [refresh refresh-all]]
   [com.dept24c.vivo.bristlecone-server :as bs]
   [com.dept24c.vivo.state-schema :as ss]
   [deercreeklabs.capsule.logging :as logging]
   [puget.printer :refer [cprint]]))

(def default-server-port 12345)

(def stop-server nil)

(defn configure-logging []
  (logging/add-log-reporter! :println logging/println-reporter)
  (logging/set-log-level! :debug))

(defn stop []
  (if stop-server
    (do
      (println "Stopping server...")
      (stop-server))
    (println "Server is not running.")))

(defn start
  ([]
   (start default-server-port))
  ([port]
   (configure-logging)
   (alter-var-root #'stop-server
                   (constantly (bs/bristlecone-server port ss/state-schema)))))

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
