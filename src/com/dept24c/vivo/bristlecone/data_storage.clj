(ns com.dept24c.vivo.bristlecone.data-storage
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [com.dept24c.vivo.bristlecone.block-ids :as block-ids]
   [com.dept24c.vivo.bristlecone.data-block-storage :as dbs]
   [com.dept24c.vivo.bristlecone.tx-fns :as tx-fns]
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]))

(defprotocol IDataStorageInternals
  (<read-data [this db-id schema])
  (<write-data [this schema state]))

(l/def-record-schema state-store-info-schema
  [:schema-fp :required l/long-schema]
  [:block-ids (l/array-schema l/string-schema)]
  [:inline-bytes l/bytes-schema])

(defrecord DataStorage [data-block-storage sub-map->op-cache]
  u/ISchemaStore
  (<schema->fp [this schema]
    (u/<schema->fp data-block-storage schema))

  (<fp->schema [this fp]
    (u/<fp->schema data-block-storage fp))

  IDataStorageInternals
  (<read-data [this db-id schema]
    (au/go
      (let [info (au/<? (u/<read-data-block data-block-storage db-id
                                            state-store-info-schema))
            {:keys [schema-fp block-ids inline-bytes]} info
            writer-schema (au/<? (u/<fp->schema this schema-fp))]
        (if inline-bytes
          (l/deserialize schema writer-schema inline-bytes)
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
            (l/deserialize schema writer-schema bytes))))))

  (<write-data [this schema state]
    (au/go
      (let [encoded (l/serialize schema state)
            info-id (au/<? (u/<allocate-data-block-id data-block-storage))
            info {:schema-fp (au/<? (u/<schema->fp this schema))}
            block-size (- u/max-data-block-bytes 50)]
        (if (< (count encoded) block-size)
          (au/<? (u/<write-data-block data-block-storage info-id
                                      state-store-info-schema
                                      (assoc info :inline-bytes encoded)))
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

  (<update [this data-id schema update-commands prefix]
    (u/<update this data-id schema update-commands prefix nil))

  (<update [this data-id schema update-commands prefix tx-fns]
    ;; TODO: Support structural sharing
    (when data-id ;; Allow nil for creating a new data item
      (u/check-data-id data-id))
    (au/go
      (let [old-state (when data-id
                        (au/<? (<read-data this data-id schema)))
            uc-ret (reduce
                    (fn [{:keys [state] :as acc} cmd]
                      (let [ret (commands/eval-cmd state cmd prefix)]
                        (-> acc
                            (assoc :state (:state ret))
                            (update :update-infos conj (:update-info ret)))))
                    {:state old-state
                     :update-infos []}
                    update-commands)
            ;; TODO: Optimize this. Don't run tx-fns on all updates.
            txf-ret (when tx-fns
                      (tx-fns/eval-tx-fns sub-map->op-cache tx-fns
                                          (:state uc-ret)))
            final-state (if tx-fns
                          (:state txf-ret)
                          (:state uc-ret))
            update-infos (concat (:update-infos uc-ret) (:update-infos txf-ret))
            new-data-id (au/<? (<write-data this schema final-state))]
        (u/sym-map new-data-id update-infos))))

  (<update-reference! [this reference schema update-commands prefix]
    (u/<update-reference! this reference schema update-commands prefix nil))

  (<update-reference! [this reference schema update-commands prefix tx-fns]
    (au/go
      (let [num-tries 10]
        (u/check-reference reference)
        (loop [num-tries-left (dec num-tries)]
          (let [data-id (au/<? (u/<get-data-id this reference))
                {:keys [new-data-id]} (au/<? (u/<update this data-id schema
                                                        update-commands
                                                        prefix tx-fns))]
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

(defn data-storage [data-block-storage]
  (let [sub-map->op-cache (sr/stockroom 500)]
    (->DataStorage data-block-storage sub-map->op-cache)))
