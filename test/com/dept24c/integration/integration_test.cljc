(ns com.dept24c.integration.integration-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [com.dept24c.vivo :as vivo]
   [com.dept24c.vivo.state-schema :as ss]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as logging])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(defn get-server-url []
  "ws://localhost:12345/state-manager")

(def sm-opts {:get-server-url get-server-url
              :sys-state-schema ss/state-schema
              :sys-state-store-name "vivo-test"
              :sys-state-store-branch "integration-test"})

(def user-bo #:user{:name "Bo Johnson"
                    :nickname "Bo"})

(defn configure-logging []
  (logging/add-log-reporter! :println logging/println-reporter)
  (logging/set-log-level! :debug))

(configure-logging)

(deftest test-subscriptions
  (au/test-async
   10000
   (ca/go
     (let [sm (vivo/state-manager sm-opts)
           msg #:msg{:user user-bo
                     :text "A msg"}
           msg2 (assoc msg :msg/text "This is great")
           last-msg-ch (ca/chan 1)
           all-msgs-ch (ca/chan 1)
           app-name-ch (ca/chan 1)]
       (au/<? (vivo/<update-state!
               sm [{:path [:sys :state/msgs]
                    :op :set
                    :arg []}
                   {:path [:sys :state/msgs -1]
                    :op :insert-after
                    :arg msg}
                   {:path [:sys :state/msgs -1]
                    :op :insert-after
                    :arg msg2}]))
       (vivo/subscribe! sm "test-sub-app-name"
                        '{app-name [:sys :state/app-name]}
                        (fn [df]
                          (if-let [app-name (df 'app-name)]
                            (ca/put! app-name-ch app-name)
                            (ca/close! all-msgs-ch))))
       (vivo/subscribe! sm "test-sub-all-msgs" '{msgs [:sys :state/msgs]}
                        (fn [df]
                          (if-let [msgs (df 'msgs)]
                            (ca/put! all-msgs-ch msgs)
                            (ca/close! all-msgs-ch))))
       (vivo/subscribe! sm "test-sub-last-msg" '{last-msg [:sys :state/msgs -1]}
                        (fn [df]
                          (if-let [last-msg (df 'last-msg)]
                            (ca/put! last-msg-ch last-msg)
                            (ca/close! last-msg-ch))))
       (is (= "test-app" (au/<? app-name-ch)))
       (is (= msg2 (au/<? last-msg-ch)))
       (is (= 2 (count (au/<? all-msgs-ch))))
       (au/<? (vivo/<update-state! sm [{:path [:sys :state/msgs -1]
                                        :op :remove
                                        :arg msg}]))
       (is (= msg (au/<? last-msg-ch)))
       (is (= 1 (count (au/<? all-msgs-ch))))
       (vivo/unsubscribe! sm "test-sub-all-msgs")
       (vivo/unsubscribe! sm "test-sub-last-msg")))))
