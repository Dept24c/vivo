(ns com.dept24c.unit.bristlecone-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [com.dept24c.vivo.bristlecone.mem-block-storage :as mem-block-storage]
   [com.dept24c.vivo.bristlecone.data-block-storage :as data-block-storage]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l])
  (:import
   (clojure.lang ExceptionInfo)))

(l/def-record-schema person-schema
  [:name l/string-schema]
  [:age l/int-schema]
  [:children (l/array-schema ::person)])

(l/def-record-schema add-op-schema
  [:in-a l/int-schema]
  [:in-b l/int-schema]
  [:result (l/maybe l/int-schema)])

(deftest test-mem-block-storage-temp-mode
  (au/test-async
   10000
   (ca/go
     (let [ms (mem-block-storage/mem-block-storage true)
           block-id "-A"
           v (ba/byte-array (range 3))]
       (is (= "-B" (au/<? (u/<allocate-block-id ms))))
       (is (= true (au/<? (u/<write-block ms block-id v))))
       (is (ba/equivalent-byte-arrays?
            v (au/<? (u/<read-block ms block-id))))))))

(deftest test-mem-block-storage-perm-mode
  (au/test-async
   10000
   (ca/go
     (let [ms (mem-block-storage/mem-block-storage false)
           block-id "A"
           v (ba/byte-array (range 3))]
       (is (= "B" (au/<? (u/<allocate-block-id ms))))
       (is (= true (au/<? (u/<write-block ms block-id v))))
       (is (ba/equivalent-byte-arrays?
            v (au/<? (u/<read-block ms block-id))))))))

(deftest test-data-block-storage-1
  (au/test-async
   10000
   (ca/go
     (let [block-storage (mem-block-storage/mem-block-storage false)
           dbs (data-block-storage/data-block-storage block-storage)
           w-schema (l/record-schema ::test
                                     [[:a l/int-schema]
                                      [:b l/string-schema]])
           r-schema (l/record-schema ::test
                                     [[:a l/int-schema]
                                      [:b l/string-schema]
                                      [:c l/string-schema]])
           data {:a 1 :b "x"}
           k "test-key"
           _ (is (= true (au/<? (u/<write-data-block dbs k w-schema data))))
           rt-data (au/<? (u/<read-data-block dbs k r-schema))
           expected (assoc data :c nil)]
       (= expected rt-data)))))

(deftest test-data-block-storage-2
  (au/test-async
   10000
   (ca/go
     (let [block-storage (mem-block-storage/mem-block-storage false)
           dbs (data-block-storage/data-block-storage block-storage)
           w-schema (l/record-schema ::test
                                     [[:a l/int-schema]
                                      [:b l/string-schema]
                                      [:c l/string-schema]])
           r-schema (l/record-schema ::test
                                     [[:a l/int-schema]
                                      [:b l/string-schema]])
           data {:a 1 :b "x" :c "z"}
           k "test-key"
           _ (is (= true (au/<? (u/<write-data-block dbs k w-schema data))))
           rt-data (au/<? (u/<read-data-block dbs k r-schema))
           expected (dissoc data :c)]
       (= expected rt-data)))))
