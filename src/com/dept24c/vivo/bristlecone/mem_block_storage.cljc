(ns com.dept24c.vivo.bristlecone.mem-block-storage
  (:require
   [clojure.string :as str]
   [com.dept24c.vivo.bristlecone.block-ids :as block-ids]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]))

(defprotocol IMemBlockStorageInternals
  (check-temp-perm [this k]))

(defrecord MemBlockStorage [act-as-temp-storage? *last-block-num *storage]
  u/IBlockStorage
  (<allocate-block-id [this]
    (au/go
      (->> (if act-as-temp-storage? dec inc)
           (swap! *last-block-num)
           (block-ids/block-num->block-id))))

  (<compare-and-set-bytes! [this reference old-bytes new-bytes]
    (au/go
      ;; Can't use clojure's compare-and-set! here b/c byte arrays
      ;; don't compare properly.
      (let [new-storage
            (swap! *storage
                   (fn [old-storage]
                     (if (ba/equivalent-byte-arrays?
                          old-bytes (get old-storage reference))
                       (assoc old-storage reference new-bytes)
                       old-storage)))]
        (ba/equivalent-byte-arrays? (get new-storage reference) new-bytes))))

  (<read-block [this k]
    (u/<read-block this k false))

  (<read-block [this k skip-cache?]
    (au/go
      (when k
        (check-temp-perm this k)
        (@*storage k))))

  (<write-block [this k data]
    (u/<write-block this k data false))

  (<write-block [this k data skip-cache?]
    (au/go
      (when-not k
        (throw (ex-info "k cannot be nil." {:k k})))
      (check-temp-perm this k)
      (when-not (ba/byte-array? data)
        (throw (ex-info (str "Data must be a byte array. Got `"
                             (or data "nil") "`.")
                        {:given-data data})))
      (swap! *storage assoc k data)
      true))

  (<delete-block [this k]
    (au/go
      (swap! *storage dissoc k)))

  IMemBlockStorageInternals
  (check-temp-perm [this k]
    (when-not (str/starts-with? k "_") ;; Ignore references
      (let [temp-k? (str/starts-with? k "-")]
        (case [act-as-temp-storage? temp-k?]
          [true false]
          (throw (ex-info (str "Key `" k "` is not a temp key.") {:k k}))

          [false true]
          (throw (ex-info (str "Key `" k "` is a temp key.") {:k k}))

          nil)))))

(defn mem-block-storage [act-as-temp-storage?]
  (let [*last-block-num (atom 0)
        *storage (atom {})]
    (->MemBlockStorage act-as-temp-storage? *last-block-num *storage)))
