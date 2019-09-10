(ns com.dept24c.vivo.bristlecone.data-block-storage
  (:require
   [com.dept24c.vivo.utils :as u]
   [com.dept24c.vivo.bristlecone.block-ids :as block-ids]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]))

(l/def-record-schema data-block-schema
  [:fp :required l/long-schema]
  [:bytes :required l/bytes-schema])

(def reference-value-schema l/string-schema)

(defn fp->key [fp]
  (str u/fp-to-schema-reference-root (block-ids/long->str fp)))

(defrecord DataBlockStorage [block-storage fp->schema-cache]
  u/ISchemaStore
  (<schema->fp [this schema]
    (when-not schema
      (throw (ex-info "schema must not be nil." {})))
    (au/go
      (let [fp (l/fingerprint64 schema)]
        (if (sr/get fp->schema-cache fp)
          fp
          (let [k (fp->key fp)
                bytes (l/serialize l/string-schema (l/pcf schema))]
            (au/<? (u/<write-block block-storage k bytes))
            (sr/put! fp->schema-cache fp schema)
            fp)))))

  (<fp->schema [this fp]
    (when-not fp
      (throw (ex-info "fp must not be nil." {})))
    (au/go
      (or (sr/get fp->schema-cache fp)
          (let [k (fp->key fp)
                schema (some->> (u/<read-block block-storage k)
                                (au/<?)
                                (l/deserialize-same l/string-schema)
                                (l/json->schema))]
            (when schema
              (sr/put! fp->schema-cache fp schema))
            schema))))

  u/IDataBlockStorage
  (<allocate-data-block-id [this]
    (u/<allocate-block-id block-storage))

  (<read-data-block [this block-id schema]
    (when-not block-id
      (throw (ex-info "block-id must not be nil." {})))
    (when-not schema
      (throw (ex-info "schema must not be nil." {})))
    (au/go
      (let [block (some->> (u/<read-block block-storage block-id)
                           (au/<?)
                           (l/deserialize-same data-block-schema))
            {bytes :bytes
             writer-fp :fp} block
            writer-schema (when block
                            (au/<? (u/<fp->schema this writer-fp)))]
        (when block
          (l/deserialize schema writer-schema bytes)))))

  (<write-data-block [this block-id schema data]
    (when-not block-id
      (throw (ex-info "block-id must not be nil." {})))
    (when-not schema
      (throw (ex-info "schema must not be nil." {})))
    (au/go
      (let [fp (au/<? (u/<schema->fp this schema))
            bytes (l/serialize schema data)
            data-block (u/sym-map fp bytes)
            encoded (l/serialize data-block-schema data-block)]
        (au/<? (u/<write-block block-storage block-id encoded)))))

  (<delete-data-block [this block-id]
    (u/<delete-block block-storage block-id))

  (<set-reference! [this reference data-id]
    (let [encoded (l/serialize reference-value-schema data-id)]
      (u/<write-block block-storage reference encoded true)))

  (<get-data-id [this reference]
    (au/go
      (some->> (u/<read-block block-storage reference true)
               (au/<?)
               (l/deserialize-same reference-value-schema))))

  (<compare-and-set! [this reference schema old new]
    (let [old-bytes (when old
                      (l/serialize schema old))
          new-bytes (l/serialize schema new)]
      (u/<compare-and-set-bytes! block-storage reference
                                 old-bytes new-bytes))))

(defn data-block-storage
  ([block-storage]
   (data-block-storage block-storage 1000))
  ([block-storage fp-cache-size]
   (let [fp->schema-cache (sr/stockroom fp-cache-size)]
     (->DataBlockStorage block-storage fp->schema-cache))))
