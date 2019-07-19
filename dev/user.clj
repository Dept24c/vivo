(ns user
  (:require
   [clojure.tools.namespace.repl :refer [refresh refresh-all]]
   [com.dept24c.vivo.server :as server]
   [com.dept24c.vivo.state-schema :as ss]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as logging]
   [puget.printer :refer [cprint]]))

(def default-server-port 12345)

(def stop-server nil)

(defn <authenticate [id secret]
  (au/go
    "user-a"))

(defn <authorized? [subject-id path]
  (au/go
    (not= :secret (first path))))

(defn configure-logging []
  (logging/add-log-reporter! :println logging/println-reporter)
  (logging/set-log-level! :debug))

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
   (configure-logging)
   (let [tfs [{:sub-map '{msgs [:msgs]}
               :f make-user-id-to-msgs
               :output-path [:user-id-to-msgs]}]
         opts {:authentication-fn <authenticate
               :authorization-fn <authorized?
               :transaction-fns tfs}]
     (alter-var-root #'stop-server
                     (constantly (server/vivo-server
                                  port "vivo-test"
                                  ss/state-schema opts))))))

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
