(ns com.dept24c.integration.integration-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [com.dept24c.vivo :as vivo]
   [com.dept24c.vivo.bristlecone-state-provider-impl :as bspi]
   [com.dept24c.vivo.state-schema :as ss]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as logging])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(defn get-server-url []
  "ws://localhost:12345/bsp")

(def user-bo #:user{:name "Bo Johnson"
                    :nickname "Bo"})

(defn configure-logging []
  (logging/add-log-reporter! :println logging/println-reporter)
  (logging/set-log-level! :debug))

(configure-logging)

(vivo/def-subscriber last-msg-subscriber
  {last-msg [:sys :state/msgs -1]}
  [sm ch]
  (ca/put! ch last-msg))

(deftest test-def-subscriber
  (au/test-async
   1000
   (ca/go
     (let [bsp (vivo/bristlecone-state-provider get-server-url ss/state-schema)
           sm (vivo/state-manager {:sys bsp})
           ch (ca/chan 1)
           sub (last-msg-subscriber sm ch)
           msg #:msg{:user user-bo
                     :text "A msg"}]
       (vivo/update-state! sm {[:sys :state/msgs -1] [:insert-after msg]})
       (is (= msg (au/<? ch)))
       (bspi/shutdown bsp)))))
