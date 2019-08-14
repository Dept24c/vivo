(ns com.dept24c.vivo.bristlecone.ddb-storage
  (:require
   [clojure.core.async :as ca]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [com.dept24c.vivo.bristlecone.db-ids :as db-ids]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.stockroom :as sr]))

(set! *warn-on-reflection* true)

(def default-client-options
  {:block-cache-size 1000
   :create-table-if-absent true})

(def last-block-num-key "_LAST_BLOCK_NUM")

(defn check-block-id [block-id]
  (when-not (string? block-id)
    (throw (ex-info (str "Block-id must be a string. Got `"
                         (or block-id "nil") "`.")
                    {:given-block-id block-id})))
  (when (db-ids/temp-db-id? block-id)
    (throw (ex-info (str "Attempt to delete temp block in "
                         "permanent storage.")
                    (u/sym-map block-id)))))

(defrecord DDBStorage [ddb table-name block-cache]
  u/IBristleconeStorage
  (<allocate-block-num [this]
    ;; TODO: Optimize this by pre-allocating a block of 100 at a time,
    ;; then issuing as needed.
    (au/go
      (let [num-tries 3]
        (loop [num-tries-left num-tries]
          (if (pos? num-tries-left)
            (let [arg {:op :UpdateItem
                       :request {:TableName table-name
                                 :Key {"k" {:S last-block-num-key}}
                                 :ExpressionAttributeValues {":one" {:N "1"}}
                                 :UpdateExpression "SET v = v + :one"
                                 :ReturnValues "UPDATED_NEW"}}
                  ret (au/<? (aws-async/invoke ddb arg))]
              (if-not (and (= :cognitect.anomalies/incorrect
                              (:cognitect.anomalies/category ret))
                           (#{(str "com.amazonaws.dynamodb.v20120810"
                                   "#ResourceNotFoundException")
                              "com.amazon.coral.validate#ValidationException"}
                            (:__type ret)))
                (-> ret :Attributes :v :N Long/parseLong)
                (let [arg {:op :PutItem
                           :request {:TableName table-name
                                     :Item {"k" {:S last-block-num-key}
                                            "v" {:N "-1"}}}}]
                  (au/<? (aws-async/invoke ddb arg))
                  (recur (dec num-tries-left)))))
            (throw (ex-info (str "<allocate-block-num failed after " num-tries
                                 " attempts.")
                            (u/sym-map num-tries table-name))))))))

  (<read-block [this block-id]
    (u/<read-block this block-id false))

  (<read-block [this block-id skip-cache?]
    (check-block-id block-id)
    (au/go
      (or (when-not skip-cache?
            (sr/get block-cache block-id))
          (let [arg {:op :GetItem
                     :request {:TableName table-name
                               :Key {"k" {:S block-id}}
                               :AttributesToGet ["v"]}}
                ret (au/<? (aws-async/invoke ddb arg))
                data (some-> ret :Item :v :B slurp ba/b64->byte-array)]
            (when-not skip-cache?
              (sr/put! block-cache block-id data))
            data))))

  (<write-block [this block-id data]
    (u/<write-block this block-id data false))

  (<write-block [this block-id data skip-cache?]
    (check-block-id block-id)
    (when-not (ba/byte-array? data)
      (throw (ex-info (str "Data must be a byte array. Got `"
                           (or data "nil") "`.")
                      {:given-data data})))
    (au/go
      (let [b64 (ba/byte-array->b64 data)
            arg {:op :PutItem
                 :request {:TableName table-name
                           :Item {"k" {:S block-id}
                                  "v" {:B b64}}}}
            ret (au/<? (aws-async/invoke ddb arg))]
        (when-not skip-cache?
          (sr/put! block-cache block-id data))
        (or (= {} ret)
            (throw (ex-info "<write-block failed."
                            (u/sym-map arg block-id ret)))))))

  (<delete-block [this block-id]
    (au/go
      (check-block-id block-id)
      (let [arg {:op :DeleteItem
                 :request {:TableName table-name
                           :Key {"k" {:S block-id}}}}
            ret (au/<? (aws-async/invoke ddb arg))]
        (sr/evict! block-cache block-id)
        (or (= {} ret)
            (throw (ex-info "<delete-block failed."
                            (u/sym-map arg block-id ret))))))))

(defn <active-table? [ddb table-name]
  (au/go
    (let [ret (au/<? (aws-async/invoke ddb {:op :DescribeTable
                                            :request {:TableName table-name}}))]
      (= "ACTIVE" (some->> ret :Table :TableStatus)))))

(defn <create-table [ddb table-name]
  (au/go
    (aws-async/invoke ddb
                      {:op :CreateTable
                       :request {:TableName table-name
                                 :KeySchema [{:AttributeName "k"
                                              :KeyType "HASH"}]
                                 :AttributeDefinitions [{:AttributeName "k"
                                                         :AttributeType "S"}]
                                 :BillingMode "PAY_PER_REQUEST"}})
    (loop [attempts-left 60]
      (if (zero? attempts-left)
        (throw
         (ex-info (str "Could not create DynamoDB table `" table-name "`.")
                  {:table-name table-name}))
        (or (au/<? (<active-table? ddb table-name))
            (do
              (ca/<! (ca/timeout 1000))
              (recur (dec attempts-left))))))))

(defn <ddb-storage
  ([table-name]
   (<ddb-storage table-name {}))
  ([table-name options]
   (au/go
     (when-not (string? table-name)
       (throw (ex-info (str "table-name argument must be a string. Got '"
                            (or table-name "nil") "`.")
                       {:given-table-name table-name})))
     (let [opts* (merge default-client-options options)
           {:keys [block-cache-size create-table-if-absent]} opts*
           ddb (aws/client {:api :dynamodb})
           block-cache (sr/stockroom block-cache-size)]
       (when (and create-table-if-absent
                  (not (au/<? (<active-table? ddb table-name))))
         (au/<? (<create-table ddb table-name)))
       (->DDBStorage ddb table-name block-cache)))))
