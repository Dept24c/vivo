(ns com.dept24c.unit.db-ids-test
  (:require
   [clojure.test :refer [deftest is]]
   [com.dept24c.vivo.bristlecone.db-ids :as db-ids]
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
            encoded (db-ids/ulong->b62 l)
            decoded (db-ids/b62->ulong s)]
        (is (= [(db-ids/ensure-long l) s]
               [decoded encoded]))))))

(deftest test-temp-db-id
  (let [db-num (db-ids/ensure-long 123)
        db-id (db-ids/db-num->db-id db-num)
        temp-db-id (db-ids/db-id->temp-db-id db-id)
        temp-db-num (db-ids/db-id->db-num temp-db-id)]
    (is (= "9B" db-id))
    (is (= "-9B" temp-db-id))
    (is (= temp-db-id (db-ids/db-id->temp-db-id db-id)))
    (is (= db-num (db-ids/db-id->db-num db-id)))
    (is (= -123 temp-db-num))
    (is (db-ids/temp-db-id? temp-db-id))
    (is (not (db-ids/temp-db-id? db-id)))
    (is (nil? (db-ids/db-id->temp-db-id nil)))))

(deftest test-earlier?
  (let [cases [[1 2 true]
               [1 10 true]
               [1 1 false]
               [10 2 false]
               [-1 -2 true]
               [-1 -100 true]
               [-2 -1 false]
               [-3 -3 false]]]
    (doseq [[db-num1 db-num2 expected] cases]
      (let [db-id1 (db-ids/db-num->db-id db-num1)
            db-id2 (db-ids/db-num->db-id db-num2)
            ret (db-ids/earlier? db-id1 db-id2)]
        (is (= [db-num1 db-num2 expected]
               [db-num1 db-num2 ret]))))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Mismatched db-ids. db-id1 is temporary but db-id2 is permanent"
         (db-ids/earlier? "-A" "B")))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Mismatched db-ids. db-id1 is permanent but db-id2 is temporary"
         (db-ids/earlier? "XX" "-YY")))))
