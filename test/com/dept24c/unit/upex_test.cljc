(ns com.dept24c.unit.upex-test
  (:require
   [clojure.test :refer [are deftest is]]
   [com.dept24c.vivo.upex :as upex]
   [com.dept24c.vivo.utils :as u])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-set
  (let [ret (upex/eval 1 [:set 2])
        expected 2]
    (is (= expected ret))))

(deftest test-assoc
  (let [ret (upex/eval {} [:assoc :key-1 45 :key-2 28])
        expected {:key-1 45, :key-2 28}]
    (is (= expected ret))))

(deftest test-assoc-bad-orig-v
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #":assoc may only be used on a map/record value"
       (upex/eval 1 [:assoc :key-1 45]))))

(deftest test-dissoc
  (let [ret (upex/eval {:a 1 :b 2 :c 3} [:dissoc :a :b])
        expected {:c 3}]
    (is (= expected ret))))

(deftest test-dissoc-bad-orig-v
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #":dissoc may only be used on a map/record value"
       (upex/eval 1 [:dissoc :key-1]))))

(deftest test-append
  (let [ret (upex/eval [0 1] [:append 2])
        expected [0 1 2]]
    (is (= expected ret))))

(deftest test-prepend
  (let [ret (upex/eval [0 1] [:prepend 2])
        expected [2 0 1]]
    (is (= expected ret))))

(deftest test+
  (let [ret (upex/eval 10 [:+ 1])
        expected 11]
    (is (= expected ret))))
