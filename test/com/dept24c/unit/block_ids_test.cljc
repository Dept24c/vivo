(ns com.dept24c.unit.block-ids-test
  (:require
   [clojure.test :refer [deftest is]]
   [com.dept24c.vivo.bristlecone.block-ids :as block-ids]
   #?(:cljs [goog.math :as gm]))
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

#?(:cljs (def Long gm/Long))

(def max-long
  #?(:clj Long/MAX_VALUE
     :cljs (gm/Long.getMaxValue)))

(deftest test-ulong->b62->long
  (let [tcases [[0 "A"]
                [1 "B"]
                [61 "9"]
                [62 "AB"]
                [63 "BB"]
                [max-long "HwIFiAxIV9K"]]]
    (doseq [tcase tcases]
      (let [[l s] tcase
            encoded (block-ids/ulong->b62 l)
            decoded (block-ids/b62->ulong s)]
        (is (= [(block-ids/ensure-long l) s]
               [decoded encoded]))))))

(deftest test-temp-block-id
  (let [block-num (block-ids/ensure-long 123)
        block-id (block-ids/block-num->block-id block-num)
        temp-block-id (block-ids/block-id->temp-block-id block-id)
        temp-block-num (block-ids/block-id->block-num temp-block-id)]
    (is (= "9B" block-id))
    (is (= "-9B" temp-block-id))
    (is (= temp-block-id (block-ids/block-id->temp-block-id block-id)))
    (is (= block-num (block-ids/block-id->block-num block-id)))
    (is (= -123 temp-block-num))
    (is (block-ids/temp-block-id? temp-block-id))
    (is (not (block-ids/temp-block-id? block-id)))
    (is (nil? (block-ids/block-id->temp-block-id nil)))))

(deftest test-earlier?
  (let [cases [[1 2 true]
               [1 10 true]
               [1 1 false]
               [10 2 false]
               [-1 -2 true]
               [-1 -100 true]
               [-2 -1 false]
               [-3 -3 false]]]
    (doseq [[block-num1 block-num2 expected] cases]
      (let [block-id1 (block-ids/block-num->block-id block-num1)
            block-id2 (block-ids/block-num->block-id block-num2)
            ret (block-ids/earlier? block-id1 block-id2)]
        (is (= [block-num1 block-num2 expected]
               [block-num1 block-num2 ret]))))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Mismatched block-ids. block-id1 is temporary but block-id2 is permanent"
         (block-ids/earlier? "-A" "B")))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Mismatched block-ids. block-id1 is permanent but block-id2 is temporary"
         (block-ids/earlier? "XX" "-YY")))))
