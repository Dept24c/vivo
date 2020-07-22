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
           topic "my-num-topic"
           msg "100"
           unsub! (vivo/subscribe-to-topic! vc :local topic cb)]
       (is (ifn? unsub!))
       (is (= nil (vivo/publish-to-topic! vc :local topic msg)))
       (is (= msg (au/<? ch)))
       (is (= true (unsub!)))))))

(deftest test-local-pub-sub-multiple-pubs
  (au/test-async
   1000
   (au/go
     (let [vc (vivo/vivo-client)
           ch (ca/chan)
           cb #(ca/put! ch %)
           topic "my-num-topic"
           msg1 "100"
           msg2 "Ignore that last message"
           unsub! (vivo/subscribe-to-topic! vc :local topic cb)]
       (is (ifn? unsub!))
       (is (= nil (vivo/publish-to-topic! vc :local topic msg1)))
       (is (= msg1 (au/<? ch)))
       (is (= nil (vivo/publish-to-topic! vc :local topic msg2)))
       (is (= msg2 (au/<? ch)))
       (is (= true (unsub!)))))))

(deftest test-local-pub-sub-multiple-subs
  (au/test-async
   1000
   (au/go
     (let [vc (vivo/vivo-client)
           ch1 (ca/chan)
           ch2 (ca/chan)
           cb1 #(ca/put! ch1 %)
           cb2 #(ca/put! ch2 %)
           topic "my-fav-topic"
           msg "blue is my favorite color"
           unsub1! (vivo/subscribe-to-topic! vc :local topic cb1)
           unsub2! (vivo/subscribe-to-topic! vc :local topic cb2)]
       (is (ifn? unsub1!))
       (is (ifn? unsub2!))
       (is (= nil (vivo/publish-to-topic! vc :local topic msg)))
       (is (= msg (au/<? ch1)))
       (is (= msg (au/<? ch2)))
       (is (= true (unsub1!)))
       (is (= true (unsub2!)))))))

(deftest test-local-pub-sub-multiple-pubs-and-multiple-subs
  (au/test-async
   1000
   (au/go
     (let [vc (vivo/vivo-client)
           ch1 (ca/chan)
           ch2 (ca/chan)
           cb1 #(ca/put! ch1 %)
           cb2 #(ca/put! ch2 %)
           topic "my-fav-topic"
           msg1 "AAAAAAA"
           msg2 "BBBBB 23432 XXXX"
           unsub1! (vivo/subscribe-to-topic! vc :local topic cb1)
           unsub2! (vivo/subscribe-to-topic! vc :local topic cb2)]
       (is (ifn? unsub1!))
       (is (ifn? unsub2!))
       (is (= nil (vivo/publish-to-topic! vc :local topic msg1)))
       (is (= msg1 (au/<? ch1)))
       (is (= msg1 (au/<? ch2)))
       (is (= nil (vivo/publish-to-topic! vc :local topic msg2)))
       (is (= msg2 (au/<? ch1)))
       (is (= msg2 (au/<? ch2)))
       (is (= true (unsub1!)))
       (is (= true (unsub2!)))))))

;; TODO: Test bad arguments, cbs that throw, etc.
