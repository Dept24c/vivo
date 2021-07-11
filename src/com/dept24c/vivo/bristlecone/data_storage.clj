(ns com.dept24c.vivo.bristlecone.data-storage
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [com.dept24c.vivo.bristlecone.block-ids :as block-ids]
   [com.dept24c.vivo.bristlecone.data-block-storage :as dbs]
   [com.dept24c.vivo.bristlecone.tx-fns :as tx-fns]
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.logging :as log]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr])
  (:import
   (java.util UUID)))

(defprotocol IDataStorageInternals
  (<read-data [this db-id schema])
  (<write-data [this schema state s3?]))

(l/def-record-schema state-store-info-schema
  [:schema-fp :required l/long-schema]
  [:block-ids (l/array-schema l/string-schema)]
  [:inline-bytes l/bytes-schema]
  [:s3-key l/string-schema])

(def s3-client (aws/client {:api :s3}))
(aws/validate-requests s3-client true)
(def s3-cache (sr/stockroom 10))

(defn <read-bytes-from-s3 [bucket k]
  (au/go
    (let [s3-arg {:op :GetObject
                  :request {:Bucket bucket
                            :Key k}}
          ret (au/<? (aws-async/invoke s3-client s3-arg))]
      (if (:cognitect.anomalies/category ret)
        (throw (ex-info (str ret)
                        (u/sym-map s3-arg bucket k ret)))
        (let [ba (ba/byte-array (:ContentLength ret))]
          (.read (:Body ret) ba)
          ba)))))

(defn <write-bytes-to-s3 [bucket k ba]
  (ca/go
    (try
      (let [s3-arg {:op :PutObject
                    :request {:ACL "private"
                              :Body ba
                              :Bucket bucket
                              :CacheControl "max-age=31622400"
                              :ContentType "application/octet-stream"
                              :Key k}}
            ret (au/<? (aws-async/invoke s3-client s3-arg))]
        (if (:cognitect.anomalies/category ret)
          (throw (ex-info (str ret)
                          (u/sym-map s3-arg bucket k ret)))
          true))
      (catch Exception e
        (log/error (u/ex-msg-and-stacktrace e))))))

(defrecord DataStorage [data-block-storage
                        sub-map->op-cache
                        s3-data-storage-bucket]
  u/ISchemaStore
  (<schema->fp [this schema]
    (u/<schema->fp data-block-storage schema))

  (<fp->schema [this fp]
    (u/<fp->schema data-block-storage fp))

  IDataStorageInternals
  (<read-data [this db-id schema]
    (au/go
      (when-let [info (au/<? (u/<read-data-block data-block-storage db-id
                                                 state-store-info-schema))]
        (let [{:keys [schema-fp block-ids inline-bytes s3-key]} info
              writer-schema (au/<? (u/<fp->schema this schema-fp))]
          (cond
            (and s3-data-storage-bucket s3-key)
            (let [ba (or (sr/get s3-cache [s3-data-storage-bucket s3-key])
                         (au/<? (<read-bytes-from-s3 s3-data-storage-bucket
                                                     s3-key)))]
              (l/deserialize schema writer-schema ba))

            inline-bytes
            (l/deserialize schema writer-schema inline-bytes)

            :else
            ;; Use loop to stay in go block
            (let [bytes (loop [i 0
                               blocks []]
                          (let [block-id (nth block-ids i)
                                block (au/<? (u/<read-data-block
                                              data-block-storage block-id
                                              l/bytes-schema))
                                new-i (inc i)
                                new-blocks (conj blocks block)]
                            (if (= (count block-ids) new-i)
                              (ba/concat-byte-arrays new-blocks)
                              (recur new-i new-blocks))))]
              (l/deserialize schema writer-schema bytes)))))))

  (<write-data [this schema data s3?]
    (au/go
      (let [encoded (l/serialize schema data)
            info-id (au/<? (u/<allocate-data-block-id data-block-storage))
            info {:schema-fp (au/<? (u/<schema->fp this schema))}
            block-size (- u/max-data-block-bytes 50)]
        (cond
          (and s3? s3-data-storage-bucket)
          (let [s3-key (.toString ^UUID (UUID/randomUUID))]
            (sr/put! s3-cache [s3-data-storage-bucket s3-key] encoded)
            ;; Don't wait for s3
            (<write-bytes-to-s3 s3-data-storage-bucket s3-key encoded)
            (au/<? (u/<write-data-block data-block-storage info-id
                                        state-store-info-schema
                                        (assoc info :s3-key s3-key))))

          (< (count encoded) block-size)
          (au/<? (u/<write-data-block data-block-storage info-id
                                      state-store-info-schema
                                      (assoc info :inline-bytes encoded)))

          :else
          (let [num-blocks (inc (quot (count encoded) block-size))
                ;; Use loop to stay in go block
                ids (loop [i 0
                           block-ids []]
                      (let [block-id (au/<? (u/<allocate-data-block-id
                                             data-block-storage))
                            new-i (inc i)
                            new-block-ids (conj block-ids block-id)
                            start (* i block-size)
                            end (if (= num-blocks new-i)
                                  (count encoded)
                                  (+ start block-size))
                            block (ba/slice-byte-array encoded start end)]
                        (au/<? (u/<write-data-block data-block-storage
                                                    block-id l/bytes-schema
                                                    block))
                        (if (= num-blocks new-i)
                          new-block-ids
                          (recur new-i new-block-ids))))]
            (au/<? (u/<write-data-block data-block-storage info-id
                                        state-store-info-schema
                                        (assoc info :block-ids ids)))))
        info-id)))

  u/IDataStorage
  (<delete-reference! [this reference]
    (u/check-reference reference)
    (u/<delete-data-block data-block-storage reference))

  (<get-in [this data-id schema path prefix]
    ;; TODO: Support structural sharing
    (u/check-data-id data-id)
    (au/go
      (some-> (<read-data this data-id schema)
              (au/<?)
              (commands/get-in-state path prefix)
              (:val))))

  (<get-in-reference [this reference schema path prefix]
    (u/check-reference reference)
    (au/go
      (when-let [data-id (au/<? (u/<get-data-id this reference))]
        (au/<? (u/<get-in this data-id schema path prefix)))))

  (<update-reference! [this reference schema update-commands prefix]
    (au/go
      (let [num-tries 10]
        (u/check-reference reference)
        (loop [num-tries-left (dec num-tries)]
          (let [data-id (au/<? (u/<get-data-id this reference))
                old-state (when data-id
                            (au/<? (<read-data this data-id schema)))
                uc-ret (reduce
                        (fn [{:keys [state] :as acc} cmd]
                          (let [ret (commands/eval-cmd state cmd prefix)]
                            (-> acc
                                (assoc :state (:state ret))
                                (update :update-infos conj
                                        (:update-info ret)))))
                        {:state old-state
                         :update-infos []}
                        update-commands)
                {:keys [update-infos state]} uc-ret
                new-data-id (au/<? (<write-data this schema state false))]
            (if (au/<? (u/<compare-and-set! this reference l/string-schema
                                            data-id new-data-id))
              new-data-id
              (if (zero? num-tries-left)
                (throw (ex-info (str "Failed to update reference `" reference
                                     "` after " num-tries " tries.")
                                (u/sym-map reference num-tries)))
                (do
                  (au/<? (ca/timeout (rand-int 100)))
                  (recur (dec num-tries-left))))))))))

  u/IDataBlockStorage
  (<compare-and-set! [this reference schema old new]
    (u/check-reference reference)
    (u/<compare-and-set! data-block-storage reference schema old new))

  (<set-reference! [this reference data-id]
    (u/check-reference reference)
    (when-not data-id
      (throw (ex-info "data-id argument can't be nil."
                      (u/sym-map reference data-id))))
    (u/<set-reference! data-block-storage reference data-id))

  (<get-data-id [this reference]
    (u/check-reference reference)
    (u/<get-data-id data-block-storage reference)))

(defn data-storage [data-block-storage s3-data-storage-bucket]
  (let [sub-map->op-cache (sr/stockroom 500)]
    (->DataStorage data-block-storage
                   sub-map->op-cache
                   s3-data-storage-bucket)))
