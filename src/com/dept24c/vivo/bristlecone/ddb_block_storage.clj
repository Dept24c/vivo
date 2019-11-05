(ns com.dept24c.vivo.bristlecone.ddb-block-storage
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [com.dept24c.vivo.bristlecone.block-ids :as block-ids]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.stockroom :as sr]))

(set! *warn-on-reflection* true)

(def default-client-options
  {:block-cache-size 1000
   :create-table-if-absent true})
(def last-block-num-key "_LAST_BLOCK_NUM")
(def max-data-block-bytes (- (* 400 1024) 20))

(defn check-block-id [block-id]
  (when-not (string? block-id)
    (throw (ex-info (str "block-id must be a string. Got `"
                         (or block-id "nil") "`.")
                    {:given-block-id block-id})))
  (when (block-ids/temp-block-id? block-id)
    (throw (ex-info (str "block-id `" block-id "` is a temporary id.")
                    (u/sym-map block-id)))))

(defrecord DDBBlockStorage [ddb table-name block-cache]
  u/IBlockStorage
  (<allocate-block-id [this]
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
                           (or (str/includes? (:__type ret)
                                              "ResourceNotFoundException")
                               (str/includes? (:__type ret)
                                              "ValidationException")))
                (-> ret :Attributes :v :N Long/parseLong
                    block-ids/block-num->block-id)
                (let [arg {:op :PutItem
                           :request {:TableName table-name
                                     :Item {"k" {:S last-block-num-key}
                                            "v" {:N "-1"}}}}]
                  (au/<? (aws-async/invoke ddb arg))
                  (recur (dec num-tries-left)))))
            (throw (ex-info (str "<allocate-block-num failed after " num-tries
                                 " attempts.")
                            (u/sym-map num-tries table-name))))))))

  (<compare-and-set-bytes! [this reference old-bytes new-bytes]
    (au/go
      (let [old-b64 (ba/byte-array->b64 old-bytes)
            new-b64 (ba/byte-array->b64 new-bytes)
            arg {:op :UpdateItem
                 :request {:TableName table-name
                           :Key {"k" {:S reference}}
                           :ExpressionAttributeValues {":oldv"
                                                       (if old-b64
                                                         {:B old-b64}
                                                         {:NULL true})
                                                       ":newv" {:B new-b64}}
                           :UpdateExpression "SET v = :newv"
                           :ConditionExpression
                           "attribute_not_exists(v) OR v = :oldv"
                           :ReturnValues "UPDATED_NEW"}}
            ret  (au/<? (aws-async/invoke ddb arg))]
        (if-not (= :cognitect.anomalies/incorrect
                   (:cognitect.anomalies/category ret))
          (= new-b64 (some-> ret :Attributes :v :B slurp))
          (cond
            (str/includes? (:__type ret) "ResourceNotFoundException")
            (au/<? (u/<write-block this reference new-bytes true))

            (str/includes? (:__type ret) "ConditionalCheckFailedException")
            false

            :else
            (throw (ex-info (str "Unknown error in UpdateItem: " (:__type ret))
                            ret)))))))

  (<read-block [this block-id]
    (u/<read-block this block-id false))

  (<read-block [this block-id skip-cache?]
    (au/go
      (when block-id
        (check-block-id block-id)
        (or (when-not skip-cache?
              (sr/get block-cache block-id))
            (let [arg {:op :GetItem
                       :request {:TableName table-name
                                 :Key {"k" {:S block-id}}
                                 :AttributesToGet ["v"]}}
                  ret (au/<? (aws-async/invoke ddb arg))
                  data (some-> ret :Item :v :B slurp ba/b64->byte-array)]
              (when (and data
                         (not skip-cache?))
                (sr/put! block-cache block-id data))
              data)))))

  (<write-block [this block-id data]
    (u/<write-block this block-id data false))

  (<write-block [this block-id data skip-cache?]
    (check-block-id block-id)
    (when (> (count data) max-data-block-bytes)
      (throw (ex-info "Data is too big to be written in one block."
                      {:data-size (count data)
                       :max-data-size max-data-block-bytes})))
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

(defn <ddb-block-storage
  ([table-name]
   (<ddb-block-storage table-name {}))
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
       (->DDBBlockStorage ddb table-name block-cache)))))
