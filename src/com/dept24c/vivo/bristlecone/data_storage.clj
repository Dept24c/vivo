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
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]))

(defrecord DataStorage [data-block-storage]
  u/IDataStorage
  (<delete-reference! [this reference]
    (u/check-reference reference)
    (u/<delete-data-block data-block-storage reference))

  (<get-in [this data-id schema path]
    ;; TODO: Support structural sharing
    (u/check-data-id data-id)
    (au/go
      (some-> (u/<read-data-block data-block-storage data-id schema)
              (au/<?)
              (commands/get-in-state path)
              (:val))))

  (<get-in-reference [this reference schema path]
    (u/check-reference reference)
    (au/go
      (let [data-id (au/<? (u/<get-data-id this reference))]
        (au/<? (u/<get-in this data-id schema path)))))

  (<update [this data-id schema update-commands]
    (u/<update this data-id schema update-commands nil))

  (<update [this data-id schema update-commands tx-fns]
    ;; TODO: Support structural sharing
    (when data-id ;; Allow nil for the creating a new data item
      (u/check-data-id data-id))
    (au/go
      (let [old (when data-id
                  (au/<? (u/<read-data-block data-block-storage
                                             data-id schema)))
            new (cond->> (reduce commands/eval-cmd old update-commands)
                  tx-fns (tx-fns/eval-tx-fns tx-fns))
            new-data-id (au/<? (u/<allocate-data-block-id data-block-storage))]
        (au/<? (u/<write-data-block data-block-storage new-data-id schema new))
        new-data-id)))

  (<update-reference! [this reference schema update-commands]
    (u/<update-reference! this reference schema update-commands nil))

  (<update-reference! [this reference schema update-commands tx-fns]
    (au/go
      (let [num-tries 100]
        (u/check-reference reference)
        (loop [num-tries-left (dec num-tries)]
          (let [data-id (au/<? (u/<get-data-id this reference))
                new-data-id (au/<? (u/<update this data-id schema
                                              update-commands tx-fns))]
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

  (<schema->fp [this schema]
    (u/<schema->fp data-block-storage schema))

  (<fp->schema [this fp]
    (u/<fp->schema data-block-storage fp))

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
  (->DataStorage data-block-storage))
