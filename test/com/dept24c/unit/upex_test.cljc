(ns com.dept24c.unit.upex-test
  (:require
   [clojure.test :refer [are deftest is]]
   [com.dept24c.vivo.upex :as upex]
   [com.dept24c.vivo.utils :as u])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-assoc
  (let [ret (upex/execute {} [:assoc :key-1 45 :key-2 28])
        expected {:key-1 45, :key-2 28}]
    (is (= expected ret))))

(deftest test-assoc-bad-orig-v
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #":assoc may only be used on a map/record value"
       (upex/execute 1 [:assoc :key-1 45]))))

(deftest test-dissoc
  (let [ret (upex/execute {:a 1 :b 2 :c 3} [:dissoc :a :b])
        expected {:c 3}]
    (is (= expected ret))))

(deftest test-dissoc-bad-orig-v
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #":dissoc may only be used on a map/record value"
       (upex/execute 1 [:dissoc :key-1]))))

(deftest test-append
  (let [ret (upex/execute [0 1] [:append 2])
        expected [0 1 2]]
    (is (= expected ret))))

(deftest test-prepend
  (let [ret (upex/execute [0 1] [:prepend 2])
        expected [2 0 1]]
    (is (= expected ret))))

(deftest test+
  (let [ret (upex/execute 10 [:+ 1])
        expected 11]
    (is (= expected ret))))
