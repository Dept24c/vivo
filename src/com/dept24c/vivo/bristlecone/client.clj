(ns com.dept24c.vivo.bristlecone.client
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.bristlecone.db-ids :as db-ids]
   [com.dept24c.vivo.bristlecone.ddb-storage :as ddb]
   [com.dept24c.vivo.bristlecone.mem-storage :as mem-storage]
   [com.dept24c.vivo.bristlecone.tx-fns :as tx-fns]
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]))

(set! *warn-on-reflection* true)

(def branch-key-root "_BRANCH_")
(def all-branches-key "_ALL_BRANCHES")
(def fp->schema-key-root "_FP-")
(def max-branch-name-len 64)

(def branch-name-schema l/string-schema)
(def all-branch-names-schema (l/array-schema branch-name-schema))

(defprotocol IBristleconeClientInternals
  (<fp->schema [this fp])
  (<schema->fp [this sch])
  (<make-log-rec [this storage db-info])
  (<temp-branch? [this branch-name])
  (<update-commands->serializable-update-commands [this cmds])
  (<serializable-update-commands->update-commands [this scmds]))

(defn branch->k [branch]
  (str branch-key-root branch))

(defn throw-branch-name-too-long [branch-name]
  (throw
   (ex-info
    (str "Branch names must be " max-branch-name-len " characters or less. "
         "Branch " branch-name " is " (count branch-name) " characters.")
    (u/sym-map branch-name))))

(defn <get-db-info [storage db-id]
  (au/go
    (some->>  db-id
              (u/<read-block storage)
              (au/<?)
              (l/deserialize-same u/db-info-schema))))

(defn <get-all-branches* [storage]
  (au/go
    ;; TODO: Implement using bristlecone array
    (some->> (u/<read-block storage all-branches-key true)
             (au/<?)
             (l/deserialize-same u/all-branches-schema))))

(defn <add-branch-name [storage branch]
  (au/go
    ;; TODO: Implement using bristlecone array
    (let [existing-branches (au/<? (<get-all-branches* storage))
          branches-bytes (->> (conj existing-branches branch)
                              (sort)
                              (l/serialize u/all-branches-schema))]
      (au/<? (u/<write-block storage all-branches-key branches-bytes true)))))

(defn <delete-branch-name [storage branch]
  (au/go
    ;; TODO: Implement using bristlecone array
    (let [new-branches (-> (<get-all-branches* storage)
                           (au/<?)
                           (set)
                           (disj branch)
                           (seq)
                           (sort))
          bytes (l/serialize u/all-branches-schema new-branches)]
      (au/<? (u/<write-block storage all-branches-key bytes true)))))

;; TODO: Validate that all `branch` args point to a valid branch
(defrecord BristleconeClient [perm-storage temp-storage storage-schema
                              fp->schema-cache path->schema-cache]
  u/IBristleconeClient
  (<create-branch! [this branch-name db-id temp-dest?]
    (au/go
      (when-not (string? branch-name)
        (throw (ex-info (str "Branch name must be a string. Got `"
                             (or branch-name "nil") "`.")
                        (u/sym-map branch-name))))
      (when-not (or (string? db-id) (nil? db-id))
        (throw (ex-info (str "db-id must be a string or nil. Got `"
                             db-id "`.")
                        (u/sym-map db-id))))
      (when (> (count branch-name) max-branch-name-len)
        (throw-branch-name-too-long branch-name))
      (let [temp-src? (and db-id (db-ids/temp-db-id? db-id))
            dest-storage (if temp-dest?
                           temp-storage
                           perm-storage)
            db-id* (if temp-dest?
                     (db-ids/db-id->temp-db-id db-id)
                     db-id)
            existing-branches (set (au/<? (<get-all-branches* dest-storage)))]
        (when (and temp-src? (not temp-dest?))
          (throw (ex-info (str "Cannot create a permanent branch from a "
                               "temporary db-id.")
                          (u/sym-map branch-name db-id temp-dest?))))
        (when (existing-branches branch-name)
          (throw (ex-info (str "A branch named `" branch-name "` already "
                               "exists in the repository.")
                          (u/sym-map branch-name db-id temp-dest?))))
        (au/<? (<add-branch-name dest-storage branch-name))
        (when (and db-id (not temp-src?) temp-dest?)
          (some->> (u/<read-block perm-storage db-id)
                   (u/<write-block temp-storage (db-ids/db-id->temp-db-id db-id))
                   (au/<?)))

        (let [k (branch->k branch-name)
              bytes (l/serialize u/branch-value-schema db-id*)]
          (au/<? (u/<write-block dest-storage k bytes true))))))

  (<delete-branch! [this branch-name]
    (au/go
      (let [tmp? (au/<? (<temp-branch? this branch-name))
            storage (if tmp?
                      temp-storage
                      perm-storage)]
        (au/<? (u/<delete-block storage (branch->k branch-name)))
        (au/<? (<delete-branch-name storage branch-name))
        (when tmp?
          (loop [db-id (au/<? (u/<get-db-id this branch-name))]
            (when (and db-id (db-ids/temp-db-id? db-id))
              (let [dbi (->> (u/<read-block temp-storage db-id)
                             (au/<?)
                             (l/deserialize-same u/db-info-schema))
                    prev-db-id (-> (:prev-db-num dbi)
                                   (db-ids/db-num->db-id))]
                (au/<? (u/<delete-block temp-storage db-id))
                (recur prev-db-id)))))
        true)))

  (<temp-branch? [this branch-name]
    (au/go
      (-> (<get-all-branches* temp-storage)
          (au/<?)
          (set)
          (get branch-name)
          (boolean))))

  (<get-in [this db-id path]
    (au/go
      (when-not (string? db-id)
        (throw (ex-info (str "Bad db-id `" db-id
                             "`. db-id must be a string.")
                        {:db-id db-id})))
      (when (and path (not (sequential? path)))
        (throw (ex-info (str "Bad path `" path "`. Path must be a sequence.")
                        {:path path})))
      (let [path* (or path [])
            storage (if (db-ids/temp-db-id? db-id)
                      temp-storage
                      perm-storage)
            dbi (->> (u/<read-block storage db-id)
                     (au/<?)
                     (l/deserialize-same u/db-info-schema))
            {:keys [data-block-num data-block-fp]} dbi
            data-wschema (au/<? (<fp->schema this data-block-fp))
            data (->> data-block-num
                      (db-ids/db-num->db-id)
                      (u/<read-block storage)
                      (au/<?)
                      (l/deserialize storage-schema data-wschema))]
        (:val (commands/get-in-state data path)))))

  (<get-all-branches [this]
    (au/go
      (let [perm-branches (au/<? (<get-all-branches* perm-storage))
            temp-branches (au/<? (<get-all-branches* temp-storage))
            all-branches (->> (concat perm-branches temp-branches)
                              (sort))]
        (when (seq all-branches)
          all-branches))))

  (<get-db-id [this branch]
    (au/go
      (let [storage (if (au/<? (<temp-branch? this branch))
                      temp-storage
                      perm-storage)
            k (branch->k branch)
            data (when k
                   (au/<? (u/<read-block storage k true)))]
        (when data
          (l/deserialize-same u/branch-value-schema data)))))

  (<get-num-commits [this branch]
    (au/go
      (let [storage (if (au/<? (<temp-branch? this branch))
                      temp-storage
                      perm-storage)]
        (or (some->> (u/<get-db-id this branch)
                     (au/<?)
                     (<get-db-info storage)
                     (au/<?)
                     (:num-prev-dbs)
                     (inc))
            0))))

  (<get-log [this branch limit]
    (au/go
      (let [storage (if (au/<? (<temp-branch? this branch))
                      temp-storage
                      perm-storage)
            dbi (->> (u/<get-db-id this branch)
                     (au/<?)
                     (<get-db-info storage)
                     (au/<?))]
        (when dbi
          (loop [out [(au/<? (<make-log-rec this storage dbi))]
                 i 1]
            (let [{:keys [prev-db-num prev-db-fp]} (peek out)]
              (if (or (not prev-db-num)
                      (= limit i))
                out
                (let [prev-dbi (au/<? (<get-db-info storage prev-db-num))]
                  (recur (conj out (au/<? (<make-log-rec
                                           this storage prev-dbi)))
                         (inc i))))))))))

  (<merge! [this source-branch target-branch]
    (au/go
      ;; TODO: Implement
      ;; Find common ancestor and replay from that point
      ;; If no common ancestor, replay whole source chain
      ))

  (<commit! [this branch update-commands msg tx-fns]
    (au/go
      ;; TODO: Optimize for parallel execution
      ;; TODO: Update the branch pointer w/ a conditional update
      ;;       If it fails, delete allocated blocks and retry
      (let [storage (if (au/<? (<temp-branch? this branch))
                      temp-storage
                      perm-storage)
            prev-db-id (au/<? (u/<get-db-id this branch))
            dbi (au/<? (<get-db-info storage prev-db-id))
            {:keys [num-prev-dbs data-block-num data-block-fp]} dbi
            data-wschema (when data-block-fp
                           (au/<? (<fp->schema this data-block-fp)))
            new-data-block-num (au/<? (u/<allocate-block-num storage))
            new-data-block-fp (au/<? (<schema->fp this storage-schema))
            new-db-num (au/<? (u/<allocate-block-num storage))
            cur-db-id (db-ids/db-num->db-id new-db-num)
            uc-block-num (au/<? (u/<allocate-block-num storage))
            uc-fp (au/<? (<schema->fp
                          this u/serializable-update-commands-schema))
            new-dbi {:data-block-num new-data-block-num
                     :data-block-fp new-data-block-fp
                     :update-commands-block-num uc-block-num
                     :update-commands-fp uc-fp
                     :msg msg
                     :timestamp-ms (u/current-time-ms)
                     :num-prev-dbs (if num-prev-dbs
                                     (inc num-prev-dbs)
                                     0)
                     :prev-db-num (when prev-db-id
                                    (db-ids/db-id->db-num prev-db-id))}
            old-state (some->> data-block-num
                               (db-ids/db-num->db-id)
                               (u/<read-block storage)
                               (au/<?)
                               (l/deserialize storage-schema data-wschema))
            new-state (->> (reduce commands/eval-cmd old-state update-commands)
                           (tx-fns/eval-tx-fns tx-fns))]
        (->> (l/serialize storage-schema new-state)
             (u/<write-block storage (db-ids/db-num->db-id new-data-block-num))
             (au/<?))
        (->> (<update-commands->serializable-update-commands
              this update-commands)
             (au/<?)
             (l/serialize u/serializable-update-commands-schema)
             (u/<write-block storage (db-ids/db-num->db-id uc-block-num))
             (au/<?))
        (->> (l/serialize u/db-info-schema new-dbi)
             (u/<write-block storage cur-db-id)
             (au/<?))
        (let [k (branch->k branch)
              data (l/serialize u/branch-value-schema cur-db-id)]
          (au/<? (u/<write-block storage k data true)))
        (u/sym-map cur-db-id prev-db-id))))

  IBristleconeClientInternals
  (<schema->fp [this sch]
    (au/go
      (when-not (l/schema? sch)
        (throw (ex-info (str "`" sch "` is not a valid lancaster schema.")
                        {:given-schema sch})))
      (let [fp (l/fingerprint64 sch)]
        (if (au/<? (<fp->schema this fp))
          fp
          (let [pcf-bytes (-> (l/pcf sch)
                              (ba/utf8->byte-array))
                k (str fp->schema-key-root (str fp))]
            (au/<? (u/<write-block perm-storage k pcf-bytes))
            (sr/put! fp->schema-cache fp sch)
            fp)))))

  (<fp->schema [this fp]
    (au/go
      (when-not (int? fp)
        (throw (ex-info (str "Given `fp` arg is not a `long`. Got `"
                             fp "`.")
                        {:given-fp fp})))
      (or (sr/get fp->schema-cache fp)
          (when-let [sch (some->> (str fp)
                                  (str fp->schema-key-root)
                                  (u/<read-block perm-storage)
                                  (au/<?)
                                  (ba/byte-array->utf8)
                                  (l/json->schema))]
            (sr/put! fp->schema-cache fp sch)
            sch))))

  (<make-log-rec [this storage db-info]
    (au/go
      (let [{:keys [update-commands-fp update-commands-block-num]} db-info
            uc-wschema (au/<? (<fp->schema this update-commands-fp))
            cmds (->> update-commands-block-num
                      (db-ids/db-num->db-id)
                      (u/<read-block storage)
                      (au/<?)
                      (l/deserialize u/serializable-update-commands-schema
                                     uc-wschema)
                      (<serializable-update-commands->update-commands this)
                      (au/<?))]
        (-> (select-keys db-info [:msg :timestamp-ms])
            (assoc :update-commands cmds)))))

  (<update-commands->serializable-update-commands [this cmds]
    (au/go
      (if-not (seq cmds)
        []
        ;; Use loop/recur to stay in single go block
        (loop [cmd (first cmds)
               i 0
               out []]
          (let [{:keys [path arg]} cmd
                arg-sch (when arg
                          (or (sr/get path->schema-cache path)
                              (let [sch (l/schema-at-path storage-schema path)]
                                (sr/put! path->schema-cache path sch)
                                sch)))
                scmd (cond-> cmd
                       arg (assoc :arg {:fp (au/<? (<schema->fp this arg-sch))
                                        :bytes (l/serialize arg-sch arg)}))
                new-i (inc i)
                new-out (conj out scmd)]
            (if (> (count cmds) new-i)
              (recur (nth cmds new-i) new-i new-out)
              new-out))))))

  (<serializable-update-commands->update-commands [this scmds]
    (au/go
      (if-not (seq scmds)
        []
        ;; Use loop/recur to stay in single go block
        (loop [scmd (first scmds)
               i 0
               out []]
          (let [{:keys [path arg]} scmd
                arg-sch (when arg
                          (or (sr/get path->schema-cache path)
                              (let [sch (l/schema-at-path storage-schema path)]
                                (sr/put! path->schema-cache path sch)
                                sch)))
                writer-arg-sch (when arg
                                 (au/<? (<fp->schema this (:fp arg))))
                arg* (when arg
                       (l/deserialize arg-sch writer-arg-sch (:bytes arg)))
                cmd (cond-> scmd
                      arg (assoc :arg arg*))
                new-i (inc i)
                new-out (conj out cmd)]
            (if (> (count scmds) new-i)
              (recur (nth scmds new-i) new-i new-out)
              new-out)))))))

(defn <bristlecone-client* [perm-storage storage-schema]
  (au/go
    ;; TODO: Check that schema matches stored schema
    (let [temp-storage (mem-storage/mem-storage true)
          fp->schema-cache (sr/stockroom 1000)
          path->schema-cache (sr/stockroom 1000)]
      (->BristleconeClient perm-storage temp-storage storage-schema
                           fp->schema-cache path->schema-cache))))

;; TODO: Validate args
(defn <bristlecone-client [repository-name storage-schema]
  (au/go
    (au/<? (<bristlecone-client*
            (au/<? (ddb/<ddb-storage repository-name))
            storage-schema))))

;; TODO: Ensure all fp<->schema stuff goes through local cache and DDB cache
;; TODO: Ensure that more than one client per table works properly
