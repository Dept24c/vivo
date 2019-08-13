(ns com.dept24c.unit.bristlecone-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [com.dept24c.vivo.bristlecone :as bc]
   [com.dept24c.vivo.bristlecone.client :as client]
   [com.dept24c.vivo.bristlecone.db-ids :as db-ids]
   [com.dept24c.vivo.bristlecone.mem-storage :as mem-storage]
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

(deftest test-mem-storage-temp-mode
  (au/test-async
   10000
   (ca/go
     (let [ms (mem-storage/mem-storage true)
           block-id "A"
           v (ba/byte-array (range 3))]
       (is (= -1 (au/<? (u/<allocate-block-num ms))))
       (is (= true (au/<? (u/<write-block ms block-id v))))
       (is (ba/equivalent-byte-arrays?
            v (au/<? (u/<read-block ms block-id))))))))

(deftest test-mem-storage-perm-mode
  (au/test-async
   10000
   (ca/go
     (let [ms (mem-storage/mem-storage false)
           block-id "A"
           v (ba/byte-array (range 3))]
       (is (= 1 (au/<? (u/<allocate-block-num ms))))
       (is (= true (au/<? (u/<write-block ms block-id v))))
       (is (ba/equivalent-byte-arrays?
            v (au/<? (u/<read-block ms block-id))))))))

(deftest test-basic-ops-perm
  (au/test-async
   1000
   (ca/go
     (let [branch "test-branch-123"
           client (au/<? (client/<bristlecone-client*
                          (mem-storage/mem-storage false)
                          person-schema))
           _ (is (nil? (au/<? (bc/<get-all-branches client))))
           ret (au/<? (bc/<create-branch! client branch nil))

           _ (is (= true ret))
           _ (is (= [branch] (au/<? (bc/<get-all-branches client))))
           _ (is (= 0 (au/<? (bc/<get-num-commits client branch))))
           _ (is (nil? (au/<? (bc/<get-log client branch))))
           alice {:name "Alice"
                  :age 50
                  :children []}
           mary {:name "Mary"
                 :age 25
                 :children []}
           bob {:name "Bob"
                :age 2
                :children []}
           update-cmds0 [{:path []
                          :op :set
                          :arg alice}]
           msg0 "Add Alice"
           ret0 (au/<? (bc/<commit! client branch update-cmds0 msg0))
           db-id0 (:cur-db-id ret0)
           _ (is (string? db-id0))
           _ (is (not (db-ids/temp-db-id? db-id0)))
           ret (au/<? (bc/<get-in client db-id0 []))
           _ (is (= alice ret))
           _ (is (= 1 (au/<? (bc/<get-num-commits client branch))))
           log0 (au/<? (bc/<get-log client branch))
           _ (is (= msg0 (-> log0 first :msg)))
           _ (is (= update-cmds0 (-> log0 first :update-commands)))
           update-cmds1 [{:path [:children 0]
                          :op :insert-after
                          :arg mary}]
           msg1 "Add Mary to Alice's children"
           ret1 (au/<? (bc/<commit! client branch update-cmds1 msg1))
           db-id1 (:cur-db-id ret1)
           ret (au/<? (bc/<get-in client db-id1 []))
           expected (update alice :children conj mary)
           _ (is (= expected ret))
           _ (is (= 2 (au/<? (bc/<get-num-commits client branch))))
           log1 (au/<? (bc/<get-log client branch))
           _ (is (= msg1 (-> log1 first :msg)))
           _ (is (= update-cmds1 (-> log1 first :update-commands)))
           new-branch "new-test-branch"
           ret (au/<? (bc/<create-branch! client new-branch db-id1))
           _ (is (true? ret))
           _ (is (= db-id1 (au/<? (bc/<get-db-id client new-branch))))
           update-cmds2 [{:path [:children 0 :children]
                          :op :set
                          :arg [bob]}]
           msg2 "Set Mary's children to a list including Bob."
           orig-branch-dbs (au/<?
                            (bc/<commit! client new-branch update-cmds2 msg2))
           new-branch-dbs (au/<?
                           (bc/<commit! client new-branch update-cmds2 msg1))]
       (is (not= orig-branch-dbs new-branch-dbs))
       (is (= [new-branch branch] (au/<? (bc/<get-all-branches client))))
       (is (au/<? (bc/<delete-branch! client new-branch)))
       (is (= [branch] (au/<? (bc/<get-all-branches client))))
       (is (au/<? (bc/<delete-branch! client branch)))
       (is (nil? (au/<? (bc/<get-all-branches client))))))))

(deftest test-basic-ops-temp
  (au/test-async
   1000
   (ca/go
     (let [branch "test-branch-123"
           client (au/<? (client/<bristlecone-client*
                          (mem-storage/mem-storage false)
                          person-schema))
           _ (is (nil? (au/<? (bc/<get-all-branches client))))
           ret (au/<? (bc/<create-branch! client branch nil true))
           _ (is (= true ret))
           _ (is (= [branch] (au/<? (bc/<get-all-branches client))))
           _ (is (= 0 (au/<? (bc/<get-num-commits client branch))))
           _ (is (nil? (au/<? (bc/<get-log client branch))))
           alice {:name "Alice"
                  :age 50
                  :children []}
           mary {:name "Mary"
                 :age 25
                 :children []}
           bob {:name "Bob"
                :age 2
                :children []}
           update-cmds0 [{:path []
                          :op :set
                          :arg alice}]
           msg0 "Add Alice"
           ret0 (au/<? (bc/<commit! client branch update-cmds0 msg0))
           db-id0 (:cur-db-id ret0)
           _ (is (string? db-id0))
           _ (is (db-ids/temp-db-id? db-id0))
           ret (au/<? (bc/<get-in client db-id0 []))
           _ (is (= alice ret))
           _ (is (= 1 (au/<? (bc/<get-num-commits client branch))))
           log0 (au/<? (bc/<get-log client branch))
           _ (is (= msg0 (-> log0 first :msg)))
           _ (is (= update-cmds0 (-> log0 first :update-commands)))
           update-cmds1 [{:path [:children 0]
                          :op :insert-after
                          :arg mary}]
           msg1 "Add Mary to Alice's children"
           ret1 (au/<? (bc/<commit! client branch update-cmds1 msg1))
           db-id1 (:cur-db-id ret1)
           expected (update alice :children conj mary)
           _ (is (= 2 (au/<? (bc/<get-num-commits client branch))))
           log1 (au/<? (bc/<get-log client branch))
           _ (is (= msg1 (-> log1 first :msg)))
           _ (is (= update-cmds1 (-> log1 first :update-commands)))
           new-branch "new-test-branch"
           ret (au/<? (bc/<create-branch! client new-branch db-id1 true))
           _ (is (true? ret))
           _ (is (= db-id1 (au/<? (bc/<get-db-id client new-branch))))
           update-cmds2 [{:path [:children 0 :children]
                          :op :set
                          :arg [bob]}]
           msg2 "Set Mary's children to a list including Bob."
           orig-branch-dbs (au/<?
                            (bc/<commit! client new-branch update-cmds2 msg2))
           new-branch-dbs (au/<?
                           (bc/<commit! client new-branch update-cmds2 msg1))]
       (is (not= orig-branch-dbs new-branch-dbs))
       (is (= [new-branch branch] (au/<? (bc/<get-all-branches client))))
       (is (au/<? (bc/<delete-branch! client new-branch)))
       (is (= [branch] (au/<? (bc/<get-all-branches client))))
       (is (au/<? (bc/<delete-branch! client branch)))
       (is (nil? (au/<? (bc/<get-all-branches client))))))))

(deftest test-tx-fns-perm
  (au/test-async
   1000
   (ca/go
     (let [branch "test-tx-fns-branch-perm"
           tx-fns [{:sub-map '{a [:in-a]
                               b [:in-b]}
                    :f (fn [{:syms [a b]}]
                         (+ a b))
                    :output-path [:result]}]
           client (au/<? (client/<bristlecone-client*
                          (mem-storage/mem-storage false)
                          add-op-schema))
           _ (is (nil? (au/<? (bc/<get-all-branches client))))
           ret (au/<? (bc/<create-branch! client branch nil))
           _ (is (= true ret))
           update-cmds0 [{:path []
                          :op :set
                          :arg {:in-a 40
                                :in-b 2}}]
           ret0 (au/<? (bc/<commit! client branch update-cmds0 "" tx-fns))
           ret (au/<? (bc/<get-in client (:cur-db-id ret0) []))
           _ (is (= 42 (:result ret)))]))))

(deftest test-tx-fns-temp
  (au/test-async
   1000
   (ca/go
     (let [branch "test-tx-fns-branch-temp"
           tx-fns [{:sub-map '{a [:in-a]
                               b [:in-b]}
                    :f (fn [{:syms [a b]}]
                         (+ a b))
                    :output-path [:result]}]
           client (au/<? (client/<bristlecone-client*
                          (mem-storage/mem-storage false)
                          add-op-schema))
           _ (is (nil? (au/<? (bc/<get-all-branches client))))
           ret (au/<? (bc/<create-branch! client branch nil true))
           _ (is (= true ret))
           update-cmds0 [{:path []
                          :op :set
                          :arg {:in-a 40
                                :in-b 2}}]
           ret0 (au/<? (bc/<commit! client branch update-cmds0 "" tx-fns))
           db-id0 (:cur-db-id ret0)
           _ (is (string? db-id0))
           ret (au/<? (bc/<get-in client db-id0 []))
           _ (is (= 42 (:result ret)))]))))
