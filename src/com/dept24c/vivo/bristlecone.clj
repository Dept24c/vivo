(ns com.dept24c.vivo.bristlecone
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr])
  (:import
   (java.nio ByteBuffer)))

(def branch-key-root "_BRANCH-")
(def fp->schema-key-root "_FP-")
(def last-block-num-key "_LAST_BLOCK_NUM")
(def max-branch-name-len 255)

(l/def-fixed-schema block-id-schema
  8)

(l/def-fixed-schema fp-schema
  8)

(l/def-record-schema log-rec-schema
  {:key-ns-type :none}
  [:timestamp l/long-schema]
  [:subject-id (l/maybe l/string-schema)]
  [:msg (l/maybe l/string-schema)]
  [:num-prev-recs l/int-schema]
  [:prev-rec-fp (l/maybe fp-schema)]
  [:prev-rec-block-id (l/maybe block-id-schema)])

(l/def-record-schema branch-info-schema
  {:key-ns-type :none}
  [:db-id (l/maybe block-id-schema)]
  [:log-rec-fp fp-schema]
  [:log-rec-block-id block-id-schema])

(defmulti <update* (fn [pschema & _]
                     (l/schema-type pschema)))

(defn bytes->long [ba]
  (let [bb ^ByteBuffer (ByteBuffer/allocate 8)]
    (.put bb ^bytes ba)
    (.flip bb)
    (.getLong bb)))

(defn long->bytes [l]
  (let [bb ^ByteBuffer (ByteBuffer/allocate 8)]
    (.putLong bb l)
    (.array bb)))

(defn branch->k [branch]
  (str branch-key-root branch))

(defn throw-branch-name-too-long [branch-name]
  (throw
   (ex-info
    (str "Branch names must be " max-branch-name-len " characters or less. "
         "Branch " branch-name " is " (count branch-name) " characters.")
    (u/sym-map branch-name))))

(defrecord BristleconeClient [storage-schema storage subject-id
                              fp->schema-cache]
  u/IBristleconeClient
  (<commit! [this branch db-id]
    (u/<commit! this branch db-id nil))

  (<commit! [this branch db-id msg]
    (au/go
      ))

  ;; (<create-branch [this source-branch target-branch]
  ;;   (when-not target-branch
  ;;     (throw (ex-info "Target branch must be provided. Got `nil`."
  ;;                     (u/sym-map source-branch target-branch))))
  ;;   (when (and source-branch (> (count source-branch) max-branch-name-len))
  ;;     (throw-branch-name-too-long source-branch))
  ;;   (when (> (count target-branch) max-branch-name-len)
  ;;     (throw-branch-name-too-long target-branch))
  ;;   (au/go
  ;;     (let [tk (branch->k target-branch)
  ;;           log-rec-fp (au/<? (u/<schema->fp-bytes this log-rec-schema))
  ;;           log-rec-block-num (au/<? (u/<allocate-block-num storage))
  ;;           bi (if source-branch
  ;;                (let [sk (branch->k source-branch)
  ;;                      sbi (->> (u/<read-block* storage sk)
  ;;                               (au/<?)
  ;;                               (l/deserialize-same branch-info-schema))
  ;;                      slog-schema (au/<? (u/<fp-bytes->schema
  ;;                                          this (:log-rec-fp sbi)))
  ;;                      slog-rec (->> (u/<read-block storage log-rec-block-id)
  ;;                                    (au/<?)
  ;;                                    (l/deserialize log-rec-schema slog-schema))
  ;;                      db-id (:db-id sbi)
  ;;                      num-prev-recs (inc (or (:num-prev-recs slog-rec) 0))
  ;;                      log-rec {:timestamp (u/current-time-ms)
  ;;                               :subject-id subject-id
  ;;                               :msg (str "Create branch from "
  ;;                                         source-branch ".")
  ;;                               :num-prev-recs num-prev-recs
  ;;                               :prev-rec-block-id (:log-rec-block-id sbi)}]
  ;;                  (u/sym-map log-rec db-id))
  ;;                (let [db-id nil
  ;;                      log-rec {:timestamp (u/current-time-ms)
  ;;                               :subject-id subject-id
  ;;                               :msg "Create inital empty branch."
  ;;                               :num-prev-recs 0}]
  ;;                  (u/sym-map db-id log-rec)))
  ;;           {:keys [log-rec db-id]} bi]
  ;;       (->> (l/serialize log-rec-schema log-rec)
  ;;            (u/<write-block storage log-rec-block-id)
  ;;            (au/<?))
  ;;       (->> (u/sym-map db-id log-rec-fp log-rec-block-id)
  ;;            (l/serialize branch-info-schema)
  ;;            (u/<write-block storage tk)
  ;;            (au/<?))
  ;;       (println 7)
  ;;       true)))

  (<delete-branch [this branch]
    (au/go
      ))

  (<get [this db-id path]
    (au/go
      ))

  (<get-branches [this]
    (au/go
      ))

  (<get-head [this branch]
    (au/go
      (let [k (branch->k branch)
            b (au/<? (u/<read-block storage k))
            _ (when-not b
                (throw (ex-info (str "Branch `" branch "` does not exist. "
                                     "Create it by calling <create-branch.")
                                (u/sym-map branch))))]
        b)))

  (<get-key-count [this db-id path]
    (au/go
      ))

  (<get-keys [this db-id path]
    (u/<get-keys this db-id path nil 0))

  (<get-keys [this db-id path limit]
    (u/<get-keys this db-id path limit 0))

  (<get-keys [this db-id path limit offset]
    (au/go
      ))

  (<get-log-count [this branch]
    (au/go
      ))

  (<get-log [this branch]
    (u/<get-log this branch nil 0))

  (<get-log [this branch limit]
    (u/<get-log this branch limit 0))

  (<get-log [this branch limit offset]
    (au/go
      ))

  (<merge [this source-branch target-branch]
    (au/go
      ))

  (<transact! [this branch update-commands]
    (u/<transact! this branch update-commands nil))

  (<transact! [this branch update-commands msg]
    (au/go
      (let [db-id (au/<? (u/<get-head this branch))
            new-db-id (au/<? (u/<update! this db-id update-commands))]
        (au/<? (u/<commit! this branch new-db-id msg))
        new-db-id)))

  (<update! [this db-id* update-commands]
    (au/go
      (let [end-i (dec (count update-commands))]
        (loop [i 0
               db-id db-id*]
          (let [{:keys [path] :as cmd} (nth update-commands i)
                parent-path (butlast path)
                parent-schema (l/schema-at-path storage-schema parent-path)
                new-db-id (au/<? (<update* parent-schema cmd db-id storage))]
            (if (= end-i i)
              new-db-id
              (recur (inc i) new-db-id)))))))

  u/IBristleconeClientInternals
  (<read-block* [this block-num]
    (au/go
      ))

  (<write-block* [this block-num data]
    (au/go
      ))

  (<schema->fp-bytes [this sch]
    (au/go
      (let [fp-long (l/fingerprint64 sch)
            fp-bytes (long->bytes fp-long)
            pcf-bytes (-> (l/pcf sch)
                          (ba/utf8->byte-array))
            k (str fp->schema-key-root(ba/byte-array->b64 fp-bytes))]
        (au/<?(u/<write-block storage k pcf-bytes))
        (sr/put fp->schema-cache fp-long sch)
        fp-bytes)))

  (<fp-bytes->schema [this fp-bytes]
    (au/go
      (let [fp-long (bytes->long fp-bytes)]
        (or (sr/get fp->schema-cache fp-long)
            (let [sch (->> (ba/byte-array->b64 fp-bytes)
                           (str fp->schema-key-root)
                           (u/<read-block storage)
                           (au/<?)
                           (ba/byte-array->utf8)
                           (l/json->schema))]
              (sr/put fp->schema-cache fp-long sch)
              sch))))))

(defrecord DDBStorage [ddb table-name]
  u/IBristleconeStorage
  (<allocate-block-num [this]
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
                (-> ret :Attributes :v :N Long/parseLong long->bytes)
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
    (au/go
      (let [arg {:op :GetItem
                 :request {:TableName table-name
                           :Key {"k" {:S block-id}}
                           :AttributesToGet ["v"]}}
            ret (au/<? (aws-async/invoke ddb arg))]
        (some-> ret :Item :v :B slurp ba/b64->byte-array))))

  (<write-block [this block-id data]
    (au/go
      (let [b64 (ba/byte-array->b64 data)
            arg {:op :PutItem
                 :request {:TableName table-name
                           :Item {"k" {:S block-id}
                                  "v" {:B b64}}}}
            ret (au/<? (aws-async/invoke ddb arg))]
        (= {} ret)))))

(defn ddb-storage [table-name]
  (let [ddb (aws/client {:api :dynamodb})]
    (->DDBStorage ddb table-name)))

(defn bristlecone-client*
  [storage-schema storage subject-id]
  (let [fp->schema-cache (sr/stockroom 100)]
    (->BristleconeClient storage-schema storage subject-id fp->schema-cache)))

;; TODO: Validate args
(defn bristlecone-client
  [store-name storage-schema subject-id]
  (bristlecone-client* storage-schema (ddb-storage store-name) subject-id))
