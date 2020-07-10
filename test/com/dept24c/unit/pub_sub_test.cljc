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
           sub-map {'foo [:local-msgs "my-num-123-msg"]}
           msg-val "lalala"
           sub-name "test123"
           ret1 (vivo/subscribe! vc sub-name sub-map cb)
           _ (is (= {'foo nil} ret1))
           ret2 (vivo/publish! vc :local-msgs "my-num-123-msg" msg-val)
           expected {'foo msg-val}]
       (is (= nil ret2))
       (is (= expected (au/<? ch)))
       (is (= nil (vivo/unsubscribe! vc sub-name)))))))

(deftest test-local-pub-sub-multiple-updates
  (au/test-async
   1000
   (au/go
     (let [vc (vivo/vivo-client)
           ch (ca/chan)
           cb #(ca/put! ch %)
           sub-map {'foo [:local-msgs "my-num-123-msg"]
                    'bar [:local :bar]}
           msg-val "lalala"
           sub-name "test123"
           ret1 (vivo/subscribe! vc sub-name sub-map cb)
           _ (is (= {'foo nil
                     'bar nil} ret1))
           ret2 (vivo/publish! vc :local-msgs "my-num-123-msg" msg-val)
           _ (is (= nil ret2))
           expected1 {'foo msg-val
                      'bar nil}
           _ (is (= expected1 (au/<? ch)))
           ret3 (au/<? (vivo/<set-state! vc [:local :bar] 123))
           expected2 {'foo nil
                      'bar 123}]
       _ (is (= expected2 (au/<? ch)))
       (is (= nil (vivo/unsubscribe! vc sub-name)))))))

(deftest test-local-pub-sub-parameterized-in-sub-map
  (au/test-async
   1000
   (au/go
     (let [vc (vivo/vivo-client)
           ret1 (au/<? (vivo/<set-state! vc [:local :a-num] 123))
           _ (is (= true ret1))
           ch (ca/chan)
           cb #(ca/put! ch %)
           sub-map '{a-num [:local :a-num]
                     foo [:local-msgs "my-num-" a-num "-msg"]}
           msg-val "lalala"
           sub-name "test123"
           ret2 (vivo/subscribe! vc sub-name sub-map cb)
           _ (is (= {'a-num 123
                     'foo nil}
                    ret2))
           ret3 (vivo/publish! vc :local-msgs "my-num-123-msg" msg-val)
           _ (is (= nil ret3))
           expected {'a-num 123
                     'foo msg-val}]
       (is (= expected (au/<? ch)))
       (is (= nil (vivo/unsubscribe! vc sub-name)))))))

(deftest test-local-pub-sub-parameterized-w-res-map
  (au/test-async
   1000
   (au/go
     (let [vc (vivo/vivo-client)
           ch (ca/chan)
           cb #(ca/put! ch %)
           resolution-map {'a-num 123}
           sub-map '{foo [:local-msgs "my-num-" a-num "-msg"]}
           msg-val "lalala"
           sub-name "test123"
           ret1 (vivo/subscribe! vc sub-name sub-map cb
                                 (u/sym-map resolution-map))
           _ (is (= {'foo nil} ret1))
           ret2 (vivo/publish! vc :local-msgs "my-num-123-msg" msg-val)]
       (is (= nil ret2))
       (is (= {'foo msg-val} (au/<? ch)))
       (is (= nil (vivo/unsubscribe! vc sub-name)))))))
