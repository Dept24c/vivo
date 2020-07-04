(ns com.dept24c.unit.pub-sub-test
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [clojure.test :refer [are deftest is]]
   [com.dept24c.vivo :as vivo]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-local-pub-sub
  (au/test-async
   1000
   (au/go
     (let [vc (vivo/vivo-client)
           ch (ca/chan)
           cb #(ca/put! ch %)
           event-name "an-event-name"
           event-str "lalala"
           unsub! (vivo/subscribe-to-event! vc :local event-name cb)]
       (vivo/publish-event! vc :local event-name event-str)
       (is (= event-str (au/<? ch)))
       (is (= true (unsub!)))))))

(deftest test-local-pub-sub-multiple-events
  (au/test-async
   1000
   (au/go
     (let [vc (vivo/vivo-client)
           ch1 (ca/chan)
           cb1 #(ca/put! ch1 %)
           ch2 (ca/chan)
           cb2 #(ca/put! ch2 %)
           event-name1 "event1"
           event-str1 "ajslfjasklf"
           event-name2 "event2"
           event-str2 "xxxxxx"
           unsub1! (vivo/subscribe-to-event! vc :local event-name1 cb1)
           unsub2! (vivo/subscribe-to-event! vc :local event-name2 cb2)]
       (vivo/publish-event! vc :local event-name1 event-str1)
       (vivo/publish-event! vc :local event-name2 event-str2)
       (is (= event-str1 (au/<? ch1)))
       (is (= event-str2 (au/<? ch2)))
       (is (= true (unsub1!)))
       (is (= true (unsub2!)))))))

(deftest test-local-pub-sub-multiple-subs-single-event
  (au/test-async
   1000
   (au/go
     (let [vc (vivo/vivo-client)
           ch1 (ca/chan)
           cb1 #(ca/put! ch1 %)
           ch2 (ca/chan)
           cb2 #(ca/put! ch2 %)
           event-name1 "event1"
           event-str1 "ajslfjasklf"
           unsub1! (vivo/subscribe-to-event! vc :local event-name1 cb1)
           unsub2! (vivo/subscribe-to-event! vc :local event-name1 cb2)]
       (vivo/publish-event! vc :local event-name1 event-str1)
       (is (= event-str1 (au/<? ch1)))
       (is (= event-str1 (au/<? ch2)))
       (is (= true (unsub1!)))
       (is (= true (unsub2!)))))))
