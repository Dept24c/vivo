(ns user
  (:require
   [clojure.tools.namespace.repl :refer [refresh refresh-all]]
   [com.dept24c.vivo.server :as server]
   [com.dept24c.vivo.state-schema :as ss]
   [deercreeklabs.capsule.logging :as logging]
   [puget.printer :refer [cprint]]))

(def default-server-port 12345)

(def stop-server nil)

(def test-initial-sys-state
  #:state{:msgs []
          :users {}
          :app-name "test-app"})

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
                   (constantly (server/vivo-server port ss/state-schema
                                                   {:initial-sys-state
                                                    test-initial-sys-state})))))

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
