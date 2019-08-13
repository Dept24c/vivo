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
              :sys-state-schema ss/state-schema})

(def user-bo {:name "Bo Johnson"
              :nickname "Bo"})
(def user-bo-id 1)

(u/configure-capsule-logging :info)

(defn join-msgs-and-users [msgs users]
  (reduce (fn [acc {:keys [user-id text] :as msg}]
            (if-not msg
              acc
              (conj acc {:user (users user-id)
                         :text text})))
          [] msgs))

(deftest test-subscriptions
  (au/test-async
   10000
   (ca/go
     (let [sm (vivo/state-manager sm-opts)]
       (try
         (let [app-name "test-app"
               msg {:user-id user-bo-id
                    :text "A msg"}
               msg2 (assoc msg :text "This is great")
               last-msg-ch (ca/chan 1)
               all-msgs-ch (ca/chan 1)
               app-name-ch (ca/chan 1)
               index-ch (ca/chan 1)
               expected-all-msgs [{:text "A msg"
                                   :user {:name "Bo Johnson"
                                          :nickname "Bo"}}
                                  {:text "This is great"
                                   :user {:name "Bo Johnson"
                                          :nickname "Bo"}}]]
           (is (= true (au/<? (vivo/<update-state!
                               sm [{:path [:sys]
                                    :op :set
                                    :arg {:app-name app-name
                                          :msgs []
                                          :users {user-bo-id user-bo}}}
                                   {:path [:sys :msgs -1]
                                    :op :insert-after
                                    :arg msg}
                                   {:path [:sys :msgs -1]
                                    :op :insert-after
                                    :arg msg2}]))))
           (vivo/subscribe! sm '{app-name [:sys :app-name]} nil
                            (fn [df]
                              (if-let [app-name (df 'app-name)]
                                (ca/put! app-name-ch app-name)
                                (ca/close! app-name-ch))))
           (vivo/subscribe! sm '{msgs [:sys :msgs]
                                 users [:sys :users]}
                            nil
                            (fn [{:syms [msgs users]}]
                              (if (seq msgs)
                                (let [msgs* (join-msgs-and-users msgs users)]
                                  (ca/put! all-msgs-ch msgs*))
                                (ca/close! all-msgs-ch))))
           (vivo/subscribe! sm '{uid->msgs [:sys :user-id-to-msgs]} nil
                            (fn [{:syms [uid->msgs]}]
                              (if (seq uid->msgs)
                                (ca/put! index-ch uid->msgs)
                                (ca/close! index-ch))))
           (vivo/subscribe! sm '{last-msg [:sys :msgs -1]} nil
                            (fn [df]
                              (if-let [last-msg (df 'last-msg)]
                                (ca/put! last-msg-ch last-msg)
                                (ca/close! last-msg-ch))))
           (is (= app-name (au/<? app-name-ch)))
           (is (= msg2 (au/<? last-msg-ch)))
           (is (= expected-all-msgs (au/<? all-msgs-ch)))
           (is (= {1 [{:text "This is great" :user-id 1}
                      {:text "A msg" :user-id 1}]}
                  (au/<? index-ch)))
           (is (= true (au/<? (vivo/<update-state! sm [{:path [:sys :msgs -1]
                                                        :op :remove}]))))
           (is (= msg (au/<? last-msg-ch)))
           (is (= 1 (count (au/<? all-msgs-ch))))
           (is (= {1 [{:text "A msg" :user-id 1}]}
                  (au/<? index-ch))))
         (finally
           (vivo/shutdown! sm)))))))

;; TODO: Test combined :sys and :local subs and updates
(deftest test-authentication
  (au/test-async
   10000
   (ca/go
     (let [sm (vivo/state-manager sm-opts)]
       (try
         (let [state-ch (ca/chan)
               subject-id "04df0587-43d7-41d8-af55-f4f6614e17eb"
               identifier "test@tester.com"
               secret "laughable"
               sub-map '{subject-id [:local :vivo/subject-id]}
               sub-id (vivo/subscribe! sm sub-map #(ca/put! state-ch %))]
           (is (= {'subject-id nil} (au/<? state-ch)))
           (vivo/log-in! sm identifier secret)
           (is (= {'subject-id subject-id} (au/<? state-ch)))
           (vivo/log-out! sm)
           (is (= {'subject-id nil} (au/<? state-ch)))
           (vivo/unsubscribe! sm sub-id))
         (finally
           (vivo/shutdown! sm)))))))


(deftest test-authorization
  (au/test-async
   10000
   (ca/go
     (let [sm (vivo/state-manager sm-opts)]
       (try
         (let [app-name "test-app"
               _ (au/<? (vivo/<set-state! sm [:sys :app-name] app-name))
               state-ch (ca/chan)
               sub-map '{app-name [:sys :app-name]
                         secret [:sys :secret]}
               sub-id (vivo/subscribe! sm sub-map #(ca/put! state-ch %))
               expected-state {'app-name app-name
                               'secret :vivo/unauthorized}]
           (is (= expected-state (au/<? state-ch)))
           (vivo/unsubscribe! sm sub-id))
         (finally
           (vivo/shutdown! sm)))))))
