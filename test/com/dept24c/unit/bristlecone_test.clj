(ns com.dept24c.unit.bristlecone-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [com.dept24c.vivo.bristlecone :as br]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]))

(defrecord BTestStorage [*last-block-num *storage]
  u/IBristleconeStorage
  (<allocate-block-num [this]
    (au/go
      (swap! *last-block-num inc)))

  (<read-block [this block-id]
    (au/go
      (@*storage block-id)))

  (<write-block [this block-id bytes]
    (au/go
      (swap! *storage assoc block-id bytes))))

(defn test-storage []
  (let [*last-block-num (atom -1)
        *storage (atom {})]
    (->BTestStorage *last-block-num *storage)))
#_
(deftest ^:this test-simple-array-inserts
  (au/test-async
   1000
   (ca/go
     (let [schema (l/array-schema l/string-schema)
           branch "test-branch-123"
           subject-id "tester"
           client (br/bristlecone-client* schema (test-storage) subject-id)
           ret (au/<? (u/<create-branch client nil branch))
           _ (is (= :foo ret))
           cases [[["a"] {:path [-1]
                          :op :insert-after
                          :arg "a"}]
                  [["a" "b"] {:path [-1]
                              :op :insert-after
                              :arg "b"}]
                  ;; [["a" "new" "b"] {:path [-1]
                  ;;                   :op :insert-before
                  ;;                   :arg "new"}]
                  ;; [["new" "a" "b"] {:path [0]
                  ;;                   :op :insert-before
                  ;;                   :arg "new"}]
                  ;; [["a" "new" "b"] {:path [0]
                  ;;                   :op :insert-after
                  ;;                   :arg "new"}]
                  ;; [["a" "b" "new"] {:path [1]
                  ;;                   :op :insert-after
                  ;;                   :arg "new"}]
                  ;; [["a" "new" "b"] {:path [1]
                  ;;                   :op :insert-before
                  ;;                   :arg "new"}]
                  ;; [["a" "b" "new"] {:path [2]
                  ;;                   :op :insert-after
                  ;;                   :arg "new"}]
                  ;; [["a" "b" "new"] {:path [10]
                  ;;                   :op :insert-after
                  ;;                   :arg "new"}]
                  ;; [["new" "a" "b"] {:path [-10]
                  ;;                   :op :insert-after
                  ;;                   :arg "new"}]
                  ;; [["a" "b" "new"] {:path [10]
                  ;;                   :op :insert-before
                  ;;                   :arg "new"}]
                  ;; [["new" "a" "b"] {:path [-10]
                  ;;                   :op :insert-before
                  ;;                   :arg "new"}]
                  ]]
       (doseq [case cases]
         (let [[expected cmd] case
               db-id (au/<? (u/<transact! client branch [cmd]))
               l (au/<? (u/<get client db-id []))]
           (is (= case [l cmd]))))))))
