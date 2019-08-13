(ns com.dept24c.vivo.bristlecone.mem-storage
  (:require
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]))

(defrecord MemStorage [act-as-temp-storage? *last-block-num *storage]
  u/IBristleconeStorage
  (<allocate-block-num [this]
    (au/go
      (swap! *last-block-num (if act-as-temp-storage? dec inc))))

  (<read-block [this block-id]
    (u/<read-block this block-id false))

  (<read-block [this block-id skip-cache?]
    (au/go
      (@*storage block-id)))

  (<write-block [this block-id data]
    (u/<write-block this block-id data false))

  (<write-block [this block-id data skip-cache?]
    (when-not (ba/byte-array? data)
      (throw (ex-info (str "Data must be a byte array. Got `"
                           (or data "nil") "`.")
                      {:given-data data})))
    (au/go
      (swap! *storage assoc block-id data)
      true))

  (<delete-block [this block-id]
    (au/go
      (swap! *storage dissoc block-id))))

(defn mem-storage [act-as-temp-storage?]
  (let [*last-block-num (atom 0)
        *storage (atom {})]
    (->MemStorage act-as-temp-storage? *last-block-num *storage)))
