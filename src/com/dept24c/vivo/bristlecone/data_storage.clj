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

(defrecord DataStorage [data-block-storage sub-map->op-cache]
  u/ISchemaStore
  (<schema->fp [this schema]
    (u/<schema->fp data-block-storage schema))

  (<fp->schema [this fp]
    (u/<fp->schema data-block-storage fp))

  u/IDataStorage
  (<delete-reference! [this reference]
    (u/check-reference reference)
    (u/<delete-data-block data-block-storage reference))

  (<get-in [this data-id schema path prefix]
    ;; TODO: Support structural sharing
    (u/check-data-id data-id)
    (au/go
      (some-> (u/<read-data-block data-block-storage data-id schema)
              (au/<?)
              (commands/get-in-state path prefix)
              (:val))))

  (<get-in-reference [this reference schema path prefix]
    (u/check-reference reference)
    (au/go
      (let [data-id (au/<? (u/<get-data-id this reference))]
        (when data-id
          (au/<? (u/<get-in this data-id schema path prefix))))))

  (<update [this data-id schema update-commands prefix]
    (u/<update this data-id schema update-commands prefix nil))

  (<update [this data-id schema update-commands prefix tx-fns]
    ;; TODO: Support structural sharing
    (when data-id ;; Allow nil for the creating a new data item
      (u/check-data-id data-id))
    (au/go
      (let [old-state (when data-id
                        (au/<? (u/<read-data-block data-block-storage
                                                   data-id schema)))
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
            new-data-id (au/<? (u/<allocate-data-block-id data-block-storage))]
        (au/<? (u/<write-data-block data-block-storage new-data-id schema
                                    final-state))
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
