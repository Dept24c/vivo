(ns com.dept24c.vivo.server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [com.dept24c.vivo.bristlecone.block-ids :as block-ids]
   [com.dept24c.vivo.bristlecone.data-block-storage :as data-block-storage]
   [com.dept24c.vivo.bristlecone.data-storage :as data-storage]
   [com.dept24c.vivo.bristlecone.ddb-block-storage :as ddb-block-storage]
   [com.dept24c.vivo.bristlecone.mem-block-storage :as mem-block-storage]
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.utils :as u]
   [crypto.password.bcrypt :as bcrypt]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.endpoint :as ep]
   [deercreeklabs.capsule.logging :as log]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.tube.connection :as tc]
   [deercreeklabs.stockroom :as sr])
  (:import
   (clojure.lang ExceptionInfo)
   (java.security SecureRandom)
   (java.util UUID)
   (java.util.concurrent ConcurrentLinkedQueue)))

(set! *warn-on-reflection* true)

(def work-factor 12)

(defprotocol IVivoServer
  (<add-subject [this arg metadata])
  (<add-subject* [this identifier secret subject-id branch conn-id])
  (<add-subject-identifier [this arg metadata])
  (<change-secret [this arg metadata])
  (<change-secret* [this branch subject-id old-secret new-secret])
  (<create-branch [this arg metadata])
  (<delete-branch [this branch metadata])
  (<fp->schema [this fp conn-id])
  (<get-all-branches [this])
  (<get-db-id [this branch])
  (<get-db-info [this branch])
  (<get-in [this db-id path])
  (<get-log [this branch limit])
  (<get-num-commits [this branch])
  (<get-schema-pcf [this arg metadata])
  (<get-state [this arg metadata])
  (<get-state-and-expanded-path [this db-id path])
  (<get-subject-id-for-identifier [this identifier branch])
  (<log-in [this arg metadata])
  (<log-in* [this identifier secret minutes-valid conn-id branch])
  (<log-in-w-token [this arg metadata])
  (<log-out [this arg metadata])
  (<log-out-w-token [this token metadata])
  (<modify-db! [this <update-fn msg subject-id branch conn-id])
  (<remove-subject-identifier [this identifier metadata])
  (<handle-request-db-changed-msg [this arg metadata])
  (<rpc [this arg metadata])
  (<set-state-source [this arg metadata])
  (<store-schema-pcf [this arg metadata])
  (<update-db [this update-cmds msg subject-id branch])
  (<update-state [this arg metadata])
  (<scmds->cmds [this scmds conn-id])
  (get-storage [this branch-or-db-id])
  (publish-msg [this arg metadata])
  (set-rpc-handler! [this rpc-name-kw handler])
  (shutdown! [this]))

(defn generate-token []
  (let [rng (SecureRandom.)
        bytes (byte-array 32)]
    (.nextBytes rng bytes)
    (ba/byte-array->b64 bytes)))

(defn branch->reference [branch]
  (str u/branch-reference-root branch))

(defn throw-branch-name-too-long [branch]
  (throw
   (ex-info
    (str "Branch names must be " u/max-branch-name-len " characters or less. "
         "Branch " branch " is " (count branch) " characters.")
    (u/sym-map branch))))

(defn <update-storage
  [data-id schema update-commands prefix branch temp-storage perm-storage]
  ;; TODO: Support structural sharing
  (when data-id ;; Allow nil for creating a new data item
    (u/check-data-id data-id))
  (au/go
    (let [dest-storage (if (str/starts-with? branch "-")
                         temp-storage
                         perm-storage)
          old-state (when data-id
                      (let [src-storage (if (str/starts-with? data-id "-")
                                          temp-storage
                                          perm-storage)]
                        (au/<?
                         (data-storage/<read-data src-storage data-id schema))))
          uc-ret (reduce
                  (fn [{:keys [state] :as acc} cmd]
                    (let [ret (commands/eval-cmd state cmd prefix)]
                      (-> acc
                          (assoc :state (:state ret))
                          (update :update-infos conj (:update-info ret)))))
                  {:state old-state
                   :update-infos []}
                  update-commands)
          {:keys [update-infos state]} uc-ret
          new-data-id (au/<?
                       (data-storage/<write-data dest-storage schema state
                                                 (= :sys prefix)))]
      (u/sym-map new-data-id update-infos state))))

(defn <add-subject-update-fn
  [requested-subject-id identifier hashed-secret dbi subject-id branch
   temp-storage perm-storage]
  (au/go
    (let [{:keys [identifier-to-subject-id-data-id
                  subject-id-to-hashed-secret-data-id]} dbi
          new-dbi (assoc dbi
                         :subject-id-to-hashed-secret-data-id
                         (:new-data-id
                          (au/<? (<update-storage
                                  subject-id-to-hashed-secret-data-id
                                  u/string-map-schema
                                  [{:path [requested-subject-id]
                                    :op :set
                                    :arg hashed-secret}]
                                  nil branch temp-storage perm-storage)))
                         :identifier-to-subject-id-data-id
                         (:new-data-id
                          (au/<? (<update-storage
                                  identifier-to-subject-id-data-id
                                  u/string-map-schema
                                  [{:path [identifier]
                                    :op :set
                                    :arg requested-subject-id}]
                                  nil branch temp-storage perm-storage))))]
      {:dbi new-dbi
       :update-infos []})))

(defn <add-subject-identifier-update-fn
  [identifier dbi subject-id branch temp-storage perm-storage]
  (au/go
    (let [{:keys [identifier-to-subject-id-data-id]} dbi
          new-dbi (assoc dbi
                         :identifier-to-subject-id-data-id
                         (:new-data-id
                          (au/<? (<update-storage
                                  identifier-to-subject-id-data-id
                                  u/string-map-schema
                                  [{:path [identifier]
                                    :op :set
                                    :arg subject-id}]
                                  nil branch temp-storage perm-storage))))]
      {:dbi new-dbi
       :update-infos []})))

(defn <remove-subject-identifier-update-fn
  [identifier dbi subject-id branch temp-storage perm-storage]
  (au/go
    (let [{:keys [identifier-to-subject-id-data-id]} dbi
          new-dbi (assoc dbi
                         :identifier-to-subject-id-data-id
                         (:new-data-id
                          (au/<? (<update-storage
                                  identifier-to-subject-id-data-id
                                  u/string-map-schema
                                  [{:path [identifier]
                                    :op :remove}]
                                  nil branch temp-storage perm-storage))))]
      {:dbi new-dbi
       :update-infos []})))

(defn <change-secret-update-fn
  [hashed-secret dbi subject-id branch temp-storage perm-storage]
  (au/go
    (let [{:keys [subject-id-to-hashed-secret-data-id
                  subject-id-to-tokens-data-id
                  token-to-token-info-data-id]} dbi
          dbi* (assoc dbi
                      :subject-id-to-hashed-secret-data-id
                      (:new-data-id
                       (au/<? (<update-storage
                               subject-id-to-hashed-secret-data-id
                               u/string-map-schema
                               [{:path [subject-id]
                                 :op :set
                                 :arg hashed-secret}]
                               nil branch temp-storage perm-storage))))
          src-storage (if (str/starts-with? subject-id-to-tokens-data-id "-")
                        temp-storage
                        perm-storage)
          ;; Invalidate existing tokens for this user
          tokens (au/<? (u/<get-in src-storage subject-id-to-tokens-data-id
                                   u/subject-id-to-tokens-schema
                                   [subject-id] nil))
          ;; Use loop here to stay in go block
          new-dbi (loop [new-dbi dbi*
                         [token & more] tokens]
                    (let [new-dbi* (assoc new-dbi
                                          :token-to-token-info-data-id
                                          (:new-data-id
                                           (au/<? (<update-storage
                                                   token-to-token-info-data-id
                                                   u/token-map-schema
                                                   [{:path [token]
                                                     :op :remove}]
                                                   nil branch temp-storage
                                                   perm-storage))))]
                      (if (seq more)
                        (recur new-dbi* more)
                        new-dbi*)))]
      {:dbi new-dbi
       :update-infos []})))

(defn <log-in-update-fn
  [token token-info dbi subject-id branch temp-storage perm-storage]
  (au/go
    (let [{:keys [token-to-token-info-data-id
                  subject-id-to-tokens-data-id]} dbi
          new-dbi (assoc dbi
                         :token-to-token-info-data-id
                         (:new-data-id
                          (au/<? (<update-storage
                                  token-to-token-info-data-id
                                  u/token-map-schema
                                  [{:path [token]
                                    :op :set
                                    :arg token-info}]
                                  nil branch temp-storage perm-storage)))
                         :subject-id-to-tokens-data-id
                         (:new-data-id
                          (au/<? (<update-storage
                                  subject-id-to-tokens-data-id
                                  u/subject-id-to-tokens-schema
                                  [{:path [subject-id -1]
                                    :op :insert-after
                                    :arg token}]
                                  nil branch temp-storage perm-storage))))]
      {:dbi new-dbi
       :update-infos []})))

(defn <log-out-update-fn
  [dbi subject-id branch temp-storage perm-storage]
  (au/go
    (let [{:keys [token-to-token-info-data-id
                  subject-id-to-tokens-data-id]} dbi
          dbi* (assoc dbi
                      :subject-id-to-tokens-data-id
                      (:new-data-id
                       (au/<? (<update-storage
                               subject-id-to-tokens-data-id
                               u/subject-id-to-tokens-schema
                               [{:path [subject-id]
                                 :op :remove}]
                               nil branch temp-storage perm-storage))))
          src-storage (if (str/starts-with? subject-id-to-tokens-data-id "-")
                        temp-storage
                        perm-storage)
          tokens (au/<? (u/<get-in src-storage subject-id-to-tokens-data-id
                                   u/subject-id-to-tokens-schema
                                   [subject-id] nil))
          ;; Use loop here to stay in go block
          new-dbi (loop [new-dbi dbi*
                         [token & more] tokens]
                    (let [new-dbi* (assoc new-dbi
                                          :token-to-token-info-data-id
                                          (:new-data-id
                                           (au/<? (<update-storage
                                                   token-to-token-info-data-id
                                                   u/token-map-schema
                                                   [{:path [token]
                                                     :op :remove}]
                                                   nil branch temp-storage
                                                   perm-storage))))]
                      (if (seq more)
                        (recur new-dbi* more)
                        new-dbi*)))]
      {:dbi new-dbi
       :update-infos []})))

(defn <delete-token-update-fn
  [token dbi subject-id branch temp-storage perm-storage]
  (au/go
    (let [{:keys [token-to-token-info-data-id
                  subject-id-to-tokens-data-id]} dbi
          src-storage (if (str/starts-with? subject-id-to-tokens-data-id "-")
                        temp-storage
                        perm-storage)
          tokens (au/<? (u/<get-in src-storage subject-id-to-tokens-data-id
                                   u/subject-id-to-tokens-schema
                                   [subject-id]
                                   nil))
          new-tokens (remove #(= token %) tokens)
          new-dbi (assoc dbi
                         :token-to-token-info-data-id
                         (:new-data-id
                          (au/<? (<update-storage
                                  token-to-token-info-data-id
                                  u/token-map-schema
                                  [{:path [token]
                                    :op :remove}]
                                  nil branch temp-storage perm-storage)))
                         ;; TODO: Improve this when `find` is implemented
                         :subject-id-to-tokens-data-id
                         (:new-data-id
                          (au/<? (<update-storage
                                  subject-id-to-tokens-data-id
                                  u/subject-id-to-tokens-schema
                                  [{:path [subject-id]
                                    :op :set
                                    :arg new-tokens}]
                                  nil branch temp-storage
                                  perm-storage))))]
      {:dbi new-dbi
       :update-infos []})))

(defn <update-state-update-fn
  [state-schema update-cmds dbi subject-id branch temp-storage perm-storage]
  (au/go
    (let [{:keys [data-id]} dbi
          ret (au/<? (<update-storage data-id state-schema
                                      update-cmds :sys branch
                                      temp-storage perm-storage))
          {:keys [new-data-id update-infos state]} ret]
      {:dbi (assoc dbi :data-id new-data-id)
       :update-infos update-infos
       :update-state-rpc? true
       :state state})))

(defn <delete-branch* [branch storage]
  (au/go
    (let [all-branches (au/<? (u/<get-in-reference
                               storage u/all-branches-reference
                               u/all-branches-schema nil nil))
          new-branches (or (-> (set all-branches)
                               (disj branch)
                               (seq))
                           [])
          branch-reference (branch->reference branch)]
      ;; TODO: Improve this when `find` is implemented
      (au/<? (u/<update-reference! storage u/all-branches-reference
                                   u/all-branches-schema
                                   [{:path nil
                                     :op :set
                                     :arg new-branches}]
                                   nil))
      (au/<? (u/<delete-reference! storage branch-reference))
      true)))

(defn notify-conns!
  [update-state-rpc? update-infos conn-id conn-ids db-id prev-db-id whole-state
   state-schema redaction-fn vc-ep perm-storage *conn-id->info]
  ;; For update-state, notify all conns except the originator,
  ;; who gets the information sent to them directly.
  ;; This allows local+sys updates to be atomic.
  (let [conn-ids* (if update-state-rpc?
                    (disj (set conn-ids) conn-id)
                    conn-ids)]
    (ca/go
      (try
        (doseq [conn-id* conn-ids*]
          (let [{:keys [subject-id]} (@*conn-id->info conn-id*)
                new-state (when whole-state
                            (redaction-fn subject-id whole-state))
                fp (au/<? (u/<schema->fp perm-storage state-schema))
                bytes (when whole-state
                        (l/serialize state-schema new-state))
                serialized-state (when whole-state
                                   (u/sym-map fp bytes))
                info (u/sym-map db-id prev-db-id serialized-state
                                subject-id update-infos)]
            (ep/send-msg vc-ep conn-id* :db-changed info)))
        (catch Exception e
          (log/error (str "Error in notify-conns!:\n"
                          (u/ex-msg-and-stacktrace e))))))))

(defn <modify-db!*
  [branch conn-id subject-id <update-fn msg redaction-fn
   state-schema vc-ep temp-storage perm-storage *branch->info *conn-id->info]
  (au/go
    (let [num-tries 10
          {:keys [conn-ids]} (@*branch->info branch)
          branch-reference (branch->reference branch)
          dest-storage (if (str/starts-with? branch "-")
                         temp-storage
                         perm-storage)]
      ;; Use loop to stay in go block
      (loop [num-tries-left (dec num-tries)]
        (let [prev-db-id (when (and dest-storage branch-reference)
                             (au/<? (u/<get-data-id dest-storage branch-reference)))
              src-storage (cond
                            (nil? prev-db-id) nil
                            (str/starts-with? prev-db-id "-") temp-storage
                            :else perm-storage)
              prev-dbi (when src-storage
                         (au/<? (u/<get-in src-storage prev-db-id
                                           u/db-info-schema nil nil)))
              {:keys [num-prev-dbs data-id]} prev-dbi
              uf-ret (au/<? (<update-fn prev-dbi subject-id branch temp-storage
                                        perm-storage))
              {:keys [dbi update-infos update-state-rpc?]} uf-ret
              new-dbi (assoc dbi
                             :msg msg
                             :timestamp-ms (u/current-time-ms)
                             :num-prev-dbs (if num-prev-dbs
                                             (inc num-prev-dbs)
                                             0)
                             :prev-db-id prev-db-id)
              update-ret (au/<? (<update-storage prev-db-id
                                                 u/db-info-schema
                                                 [{:path []
                                                   :op :set
                                                   :arg new-dbi}]
                                                 nil branch temp-storage
                                                 perm-storage))
              new-db-id (:new-data-id update-ret)
              whole-state (:state uf-ret)]
          (if (au/<? (u/<compare-and-set! dest-storage branch-reference
                                          l/string-schema
                                          prev-db-id
                                          new-db-id))
            (do
              (notify-conns! update-state-rpc? update-infos conn-id conn-ids
                             new-db-id prev-db-id whole-state state-schema
                             redaction-fn vc-ep perm-storage *conn-id->info)
              (u/sym-map new-db-id prev-db-id whole-state update-infos))
            (if (zero? num-tries-left)
              (throw
               (ex-info (str "Failed to commit to branch `" branch-reference
                             "` after " num-tries " tries.")
                        (u/sym-map branch-reference num-tries)))
              (do
                (au/<? (ca/timeout (rand-int 100)))
                (recur (dec num-tries-left))))))))))

(defn start-modify-db-loop
  [^ConcurrentLinkedQueue q redaction-fn state-schema vc-ep temp-storage
   perm-storage *branch->info *conn-id->info]
  (ca/go
    (while true
      (try
        (if-let [info (.poll q)]
          (let [{:keys [cb branch conn-id subject-id <update-fn msg]} info]
            (try
              (cb (au/<? (<modify-db!* branch conn-id subject-id <update-fn msg
                                       redaction-fn state-schema vc-ep
                                       temp-storage perm-storage *branch->info
                                       *conn-id->info)))
              (catch Exception e
                (cb e))))
          (ca/<! (ca/timeout 5)))
        (catch Exception e
          (log/error (str "Unexpected error in txn-loop:\n"
                          (u/ex-msg-and-stacktrace e))))))))

(defn <ks-at-path [kw <get-at-path p full-path]
  (au/go
    (let [coll (au/<? (<get-at-path p))]
      (cond
        (map? coll)
        (keys coll)

        (sequential? coll)
        (range (count coll))

        (nil? coll)
        []

        :else
        (throw
         (ex-info
          (str "`" kw "` is in path, but there is not "
               "a collection at " p ".")
          {:full-path full-path
           :missing-collection-path p
           :value coll}))))))

(defn <count-at-path [<get-at-path p full-path]
  (au/go
    (let [coll (au/<? (<get-at-path p))]
      (cond
        (or (map? coll) (sequential? coll))
        (count coll)

        (nil? coll)
        0

        :else
        (throw
         (ex-info
          (str "`:vivo/count` terminates path, but "
               "there is not a collection at " p ".")
          {:full-path full-path
           :missing-collection-path p
           :value coll}))))))

(defn <do-concat [<get-at-path p full-path]
  (au/go
    (let [seqs (au/<? (<get-at-path p))]
      (when (and (not (nil? seqs))
                 (or (not (sequential? seqs))
                     (not (sequential? (first seqs)))))
        (throw
         (ex-info
          (str "`:vivo/concat` terminates path, but there "
               "is not a sequence of sequences at "
               p ".")
          {:full-path full-path
           :missing-collection-path p
           :value seqs})))
      (apply concat seqs))))

(defn <do-login
  [vs conn-id branch identifier secret login-identifier-case-sensitive?
   login-lifetime-mins *conn-id->info *branch->info]
  (au/go
    (u/check-secret-len secret)
    (let [identifier* (if login-identifier-case-sensitive?
                        identifier
                        (str/lower-case identifier))
          db-info (au/<? (<get-db-info vs branch))
          {id->sid-data-id :identifier-to-subject-id-data-id
           sid->hs-data-id :subject-id-to-hashed-secret-data-id} db-info
          subject-id (when id->sid-data-id
                       (let [src-storage (get-storage vs id->sid-data-id)]
                         (au/<? (u/<get-in src-storage id->sid-data-id
                                           u/string-map-schema
                                           [identifier*] nil))))
          hashed-secret (when (and subject-id sid->hs-data-id)
                          (let [src-storage (get-storage vs sid->hs-data-id)]
                            (au/<? (u/<get-in src-storage sid->hs-data-id
                                              u/string-map-schema
                                              [subject-id] nil))))]

      (if-not (and hashed-secret
                   (bcrypt/check secret hashed-secret))
        {:subject-id nil
         :token nil}
        (let [token (generate-token)
              expiration-time-mins (+ (u/ms->mins (u/current-time-ms))
                                      login-lifetime-mins)
              token-info (u/sym-map expiration-time-mins subject-id)
              _ (when conn-id
                  (swap! *conn-id->info update conn-id
                         assoc :subject-id subject-id)
                  (swap! *branch->info
                         update branch
                         update :subject-id->conn-ids
                         update subject-id
                         (fn [conn-ids]
                           (conj (or conn-ids #{}) conn-id))))
              ret (au/<? (<modify-db! vs (partial <log-in-update-fn token
                                                  token-info)
                                      "Log in" subject-id branch conn-id))]
          {:db-id (:new-db-id ret)
           :subject-id subject-id
           :token token})))))

(defn <log-out-subject!
  [vs subject-id branch *branch->info *conn-id->info]
  (au/go
    (let [conn-ids (some-> (@*branch->info branch)
                           (:subject-id->conn-ids)
                           (get subject-id))]
      (swap! *branch->info
             update branch
             update :subject-id->conn-ids
             dissoc subject-id)
      (doseq [conn-id conn-ids]
        (swap! *conn-id->info
               update conn-id
               dissoc :subject-id)
        (au/<? (<modify-db! vs <log-out-update-fn "Log out"
                            subject-id branch nil)))
      true)))

(defn <do-add-subject
  [vs identifier secret subject-id branch conn-id work-factor
   login-identifier-case-sensitive?]
  (au/go
    (u/check-secret-len secret)
    (let [hashed-secret (bcrypt/encrypt secret work-factor)
          identifier* (if login-identifier-case-sensitive?
                        identifier
                        (str/lower-case identifier))
          ret (au/<? (<modify-db! vs (partial <add-subject-update-fn subject-id
                                              identifier* hashed-secret)
                                  (str "Add subject " subject-id)
                                  subject-id branch conn-id))]
      {:db-id (:new-db-id ret)
       :subject-id subject-id})))

(defn <do-change-secret! [vs branch subject-id old-secret new-secret]
  (au/go
    (u/check-secret-len old-secret)
    (u/check-secret-len new-secret)
    (let [branch-reference (branch->reference branch)
          storage (get-storage vs branch)
          db-id (when (and storage branch-reference)
                    (au/<? (u/<get-data-id storage branch-reference)))
          db-info (au/<? (u/<get-in storage db-id u/db-info-schema nil nil))
          {sid->hs-data-id :subject-id-to-hashed-secret-data-id} db-info
          hashed-old-secret (when (and subject-id old-secret)
                              (au/<? (u/<get-in storage sid->hs-data-id
                                                u/string-map-schema
                                                [subject-id] nil)))
          old-secret-ok? (if-not hashed-old-secret
                           true
                           (bcrypt/check old-secret hashed-old-secret))]
      (if-not old-secret-ok?
        false
        (let [hashed-new-secret (bcrypt/encrypt new-secret work-factor)]
          (-> (au/<? (<modify-db! vs (partial <change-secret-update-fn
                                              hashed-new-secret)
                                  "Change secret" subject-id branch nil))
              :new-db-id))))))

(defn <do-update-state
  [vs arg metadata authorization-fn redaction-fn state-schema perm-storage
   *conn-id->info]
  (au/go
    (let [{:keys [conn-id]} metadata
          {:keys [subject-id branch]} (@*conn-id->info conn-id)
          update-cmds (au/<? (<scmds->cmds vs arg conn-id))
          all-authed? (if (empty? update-cmds)
                        true
                        (loop [i 0] ; Use loop to stay in same go block
                          (let [{:keys [path arg]} (nth update-cmds i)
                                auth-ret (authorization-fn subject-id path
                                                           :write arg)
                                authed? (if (au/channel? auth-ret)
                                          (au/<? auth-ret)
                                          auth-ret)
                                new-i (inc i)]
                            (cond
                              (not authed?) false
                              (= (count update-cmds) new-i) true
                              :else (recur new-i)))))]
      (if-not all-authed?
        :vivo/unauthorized
        (let [ret (au/<? (<modify-db! vs
                                      (partial <update-state-update-fn
                                               state-schema update-cmds)
                                      "Update state" subject-id branch conn-id))
              {:keys [new-db-id prev-db-id whole-state update-infos]} ret
              state (redaction-fn subject-id whole-state)
              fp (au/<? (u/<schema->fp perm-storage state-schema))
              bytes (l/serialize state-schema state)
              serialized-state (u/sym-map fp bytes)]
          {:db-id new-db-id
           :prev-db-id prev-db-id
           :serialized-state serialized-state
           :update-infos update-infos})))))

(defn <log-in-w-token*
  [vivo-server state-schema redaction-fn vc-ep perm-storage token metadata
   *conn-id->info]
  (au/go
    (let [{:keys [conn-id]} metadata
          {:keys [*conn-id->info *branch->info]} vivo-server
          {:keys [branch]} (@*conn-id->info conn-id)
          db-info (au/<? (<get-db-info vivo-server branch))
          {:keys [token-to-token-info-data-id db-id]} db-info
          info (when token-to-token-info-data-id
                 (let [src-storage (get-storage vivo-server
                                                token-to-token-info-data-id)]
                   (au/<? (u/<get-in src-storage token-to-token-info-data-id
                                     u/token-map-schema [token] nil))))
          {:keys [expiration-time-mins subject-id]} info
          now-mins (-> (u/current-time-ms)
                       (u/ms->mins))]
      (when info
        (if (>= now-mins expiration-time-mins)
          (let [ret (au/<? (<modify-db! vivo-server
                                        (partial <delete-token-update-fn token)
                                        "Delete expired token"
                                        subject-id branch conn-id))]
            {:db-id (:new-db-id ret)
             :subject-id nil
             :token nil})
          (do
            (swap! *conn-id->info update conn-id assoc :subject-id subject-id)
            (swap! *branch->info
                   update branch
                   update :subject-id->conn-ids
                   update subject-id
                   (fn [conn-ids]
                     (conj (or conn-ids #{}) conn-id)))
            ;; When subject-id changes but not the db, we need to send a new db
            ;; with the results of the redaction-fn. db-id doesn't change.
            (let [update-infos [{:norm-path [:sys]
                                 :op :set}
                                {:norm-path [:vivo/subject-id]
                                 :op :set}]
                  db-id (au/<? (<get-db-id vivo-server branch))
                  whole-state (first (au/<? (<get-state-and-expanded-path
                                             vivo-server db-id [:sys])))]
              (notify-conns! false update-infos conn-id [conn-id]
                             db-id db-id whole-state state-schema
                             redaction-fn vc-ep perm-storage *conn-id->info)
              (u/sym-map db-id subject-id token))))))))

(defn <set-state-source* [vivo-server source metadata]
  (au/go
    (let [{:keys [conn-id]} metadata
          {:keys [perm-storage state-schema
                  *branch->info *conn-id->info]} vivo-server
          perm-branch (:branch/name source)
          branch (if perm-branch
                   (let [branch-reference (branch->reference perm-branch)
                         storage (get-storage vivo-server perm-branch)]
                     (when-not (au/<? (u/<get-data-id storage branch-reference))
                       (au/<? (<create-branch
                               vivo-server
                               {:branch perm-branch
                                :db-id nil}
                               metadata)))
                     perm-branch)
                   (let [branch* (str "-temp-branch-" (rand-int 1e9))]
                     (au/<? (<create-branch
                             vivo-server {:branch branch*
                                          :db-id (:temp-branch/db-id source)}
                             metadata))
                     branch*))]
      (swap! *conn-id->info update conn-id assoc
             :branch branch :temp-branch? (not perm-branch))
      (swap! *branch->info update branch
             (fn [{:keys [conn-ids] :as info}]
               (if conn-ids
                 (update info :conn-ids conj conn-id)
                 (assoc info :conn-ids #{conn-id}))))
      true)))

(defn publish-msg* [vc-ep *conn-id->info arg metadata]
  (let [{:keys [conn-id]} metadata
        conn-ids (keys @*conn-id->info)]
    (doseq [conn-id* (disj (set conn-ids) conn-id)]
      (ep/send-msg vc-ep conn-id* :publish-msg arg))
    true))

(defrecord VivoServer [authorization-fn
                       login-identifier-case-sensitive?
                       login-lifetime-mins
                       modify-q
                       path->schema-cache
                       perm-storage
                       rpcs
                       vc-ep
                       redaction-fn
                       repository-name
                       state-schema
                       stop-server
                       temp-storage
                       *conn-id->info
                       *branch->info
                       *rpc->handler]
  IVivoServer
  (<modify-db! [this <update-fn msg subject-id branch conn-id]
    (au/go
      (let [modify-ch (ca/chan)
            cb #(ca/put! modify-ch %)
            update-info (u/sym-map cb branch conn-id subject-id <update-fn msg)
            _ (.add ^ConcurrentLinkedQueue modify-q update-info)
            change-info (au/<? modify-ch)]
        change-info)))

  (<get-db-info [this branch]
    (au/go
      (let [branch-reference (branch->reference branch)
            dest-storage (get-storage this branch)
            db-id (when (and dest-storage branch-reference)
                    (au/<? (u/<get-data-id dest-storage branch-reference)))
            src-storage (get-storage this db-id)]
        (when (and src-storage db-id)
          (some-> (u/<get-in src-storage db-id u/db-info-schema nil nil)
                  (au/<?)
                  (assoc :db-id db-id))))))

  (<add-subject [this arg metadata]
    (let [{:keys [identifier secret subject-id]
           :or {subject-id (.toString ^UUID (UUID/randomUUID))}} arg
          {:keys [conn-id]} metadata
          {:keys [branch]} (@*conn-id->info conn-id)]
      (u/check-secret-len secret)
      (<do-add-subject this identifier secret subject-id branch conn-id
                       work-factor login-identifier-case-sensitive?)))

  (<add-subject* [this identifier secret subject-id branch conn-id]
    (<do-add-subject this identifier secret subject-id branch conn-id
                     work-factor login-identifier-case-sensitive?))

  (<add-subject-identifier [this identifier metadata]
    (au/go
      (let [{:keys [conn-id]} metadata
            {:keys [branch subject-id]} (@*conn-id->info conn-id)
            identifier* (if login-identifier-case-sensitive?
                          identifier
                          (str/lower-case identifier))]
        (if-not subject-id
          false ;; Must be logged in
          (-> (au/<? (<modify-db! this
                                  (partial <add-subject-identifier-update-fn
                                           identifier*)
                                  (str "Add subject indentifier `" identifier*)
                                  subject-id branch conn-id))
              :new-db-id)))))

  (<remove-subject-identifier [this identifier metadata]
    (au/go
      (let [{:keys [conn-id]} metadata
            {:keys [branch subject-id]} (@*conn-id->info conn-id)
            db-info (au/<? (<get-db-info this branch))
            id->sid-data-id (:identifier-to-subject-id-data-id db-info)
            identifier* (if login-identifier-case-sensitive?
                          identifier
                          (str/lower-case identifier))
            src-storage (get-storage this id->sid-data-id)
            id-subject-id (au/<? (u/<get-in src-storage id->sid-data-id
                                            u/string-map-schema
                                            [identifier*] nil))
            my-identifier? (and subject-id
                                (= subject-id id-subject-id))]
        (if-not my-identifier?
          false
          (-> (<modify-db! this
                           (partial <remove-subject-identifier-update-fn
                                    identifier*)
                           (str "Remove subject indentifier `" identifier*)
                           subject-id branch conn-id)
              (au/<?)
              (:new-db-id))))))

  (<change-secret* [this branch subject-id old-secret new-secret]
    (<do-change-secret! this branch subject-id old-secret new-secret))

  (<change-secret [this arg metadata]
    (let [{:keys [old-secret new-secret]} arg
          {:keys [conn-id]} metadata
          {:keys [branch subject-id]} (@*conn-id->info conn-id)]
      (<change-secret* this branch subject-id old-secret new-secret)))

  (<create-branch [this arg metadata]
    (au/go
      (let [{:keys [branch db-id]} arg
            {:keys [subject-id]} metadata
            _ (when (empty? branch)
                (throw (ex-info "Branch name is nil or empty." arg)))
            _ (when (> (count branch) u/max-branch-name-len)
                (throw-branch-name-too-long branch))
            temp-dest? (str/starts-with? branch "-")
            src-storage (get-storage this db-id)
            dest-storage (get-storage this branch)
            temp-src? (and db-id (block-ids/temp-block-id? db-id))
            branch-reference (branch->reference branch)
            _ (when (and db-id
                         (not
                          (try
                            (au/<? (u/<get-in src-storage db-id
                                              u/db-info-schema
                                              [:data-id] nil))
                            (catch ExceptionInfo e
                              (if (re-find #"not match given writer schema"
                                           (ex-message e))
                                false
                                (throw e))))))
                (throw (ex-info (str "Source db-id `" db-id "` does not exist.")
                                arg)))
            _ (when (and temp-src? (not temp-dest?))
                (throw (ex-info (str "Cannot create a permanent branch from a "
                                     "temporary db-id.")
                                (u/sym-map branch db-id temp-dest?))))
            _ (when (au/<? (u/<get-data-id dest-storage branch-reference))
                (throw (ex-info (str "A branch named `" branch "` already "
                                     "exists in the repository.")
                                (u/sym-map branch db-id temp-dest?))))
            _ (au/<? (u/<update-reference! dest-storage u/all-branches-reference
                                           u/all-branches-schema
                                           [{:path [-1]
                                             :op :insert-after
                                             :arg branch}]
                                           nil))
            db-id* (or db-id
                       (let [create-db-ch (ca/chan)
                             cb #(ca/put! create-db-ch %)
                             default-data (l/default-data state-schema)
                             update-cmds [{:path [:sys]
                                           :op :set
                                           :arg default-data}]
                             <update-fn (partial <update-state-update-fn
                                                 state-schema update-cmds)
                             msg "Create initial db for branch"
                             create-info (u/sym-map cb branch subject-id
                                                    <update-fn msg)]
                         (.add ^ConcurrentLinkedQueue modify-q create-info)
                         (:new-db-id (au/<? create-db-ch))))]
        (au/<? (u/<set-reference! dest-storage branch-reference db-id*)))))

  (<delete-branch [this branch metadata]
    (<delete-branch* branch (get-storage this branch)))

  (<fp->schema [this fp conn-id]
    (au/go
      (or (au/<? (u/<fp->schema perm-storage fp))
          (let [pcf (au/<? (ep/<send-msg vc-ep conn-id
                                         :get-schema-pcf fp))]
            (l/json->schema pcf)))))

  ;; TODO: Fix <get-all-branches to use a scan.
  ;; Get rid of all-branches-reference
  (<get-all-branches [this]
    (au/go
      (let [perm-branches (au/<? (u/<get-in-reference
                                  perm-storage u/all-branches-reference
                                  u/all-branches-schema nil nil))
            temp-branches (au/<? (u/<get-in-reference
                                  temp-storage u/all-branches-reference
                                  u/all-branches-schema nil nil))
            all-branches (concat perm-branches temp-branches)]
        (seq all-branches))))

  (<get-db-id [this branch]
    (let [storage (get-storage this branch)
          branch-reference (branch->reference branch)]
      (when (and storage branch-reference)
        (u/<get-data-id storage branch-reference))))

  (<get-in [this db-id path]
    (au/go
      (let [state-and-path (au/<? (<get-state-and-expanded-path
                                   this db-id path))]
        (first state-and-path))))

  (<get-state-and-expanded-path [this db-id path]
    (au/go
      (when-not (string? db-id)
        (throw (ex-info (str "Bad db-id `" db-id
                             "`. db-id must be a string.")
                        {:db-id db-id})))
      (when (and path (not (sequential? path)))
        (throw (ex-info (str "Bad path `" path
                             "`. Path must be nil or a sequence.")
                        {:path path})))
      (let [path (or path [:sys])
            db-id-storage (get-storage this db-id)
            data-id (au/<? (u/<get-in db-id-storage db-id u/db-info-schema
                                      [:data-id] nil))
            data-id-storage (get-storage this data-id)
            <get-at-path #(u/<get-in
                           data-id-storage data-id state-schema % :sys)
            last-path-k (last path)
            join? (u/has-join? path)
            term-kw? (u/terminal-kw-ops last-path-k)]
        (cond
          (or (not data-id) (u/empty-sequence-in-path? path))
          [nil [path]]

          (and (not term-kw?) (not join?))
          (let [val (au/<? (<get-at-path path))]
            [val [path]])

          (and term-kw? (not join?))
          (let [path* (butlast path)
                val (case last-path-k
                      :vivo/keys (au/<? (<ks-at-path :vivo/keys <get-at-path
                                                     path* path))
                      :vivo/count (au/<? (<count-at-path <get-at-path
                                                         path* path))
                      :vivo/concat (au/<? (<do-concat <get-at-path
                                                      path* path)))]
            [val [path]])

          (and (not term-kw?) join?)
          (when-not (u/empty-sequence-in-path? path)
            (let [expanded-path (au/<? (u/<expand-path
                                        #(<ks-at-path :vivo/* <get-at-path
                                                      % path)
                                        path))
                  num-results (count expanded-path)]
              (if (zero? num-results)
                [[] []]
                ;; Use loop to stay in the go block
                (loop [out []
                       i 0]
                  (let [filled-in-path (nth expanded-path i)
                        v (au/<? (u/<get-in data-id-storage data-id state-schema
                                            filled-in-path :sys))
                        new-out (conj out v)
                        new-i (inc i)]
                    (if (= num-results new-i)
                      [new-out expanded-path]
                      (recur new-out new-i)))))))

          (and term-kw? join?)
          (let [expanded-path (au/<? (u/<expand-path
                                      #(<ks-at-path :vivo/* <get-at-path
                                                    % path)
                                      (butlast path)))
                num-results (count expanded-path)]
            (if (zero? num-results)
              [[] []]
              (let [results (loop [out []
                                   i 0]
                              (let [filled-in-path (nth expanded-path i)
                                    v (au/<? (u/<get-in data-id-storage data-id
                                                        state-schema
                                                        filled-in-path :sys))
                                    new-out (conj out v)
                                    new-i (inc i)]
                                (if (= num-results new-i)
                                  new-out
                                  (recur new-out new-i))))
                    value (case last-path-k
                            :vivo/keys (range (count results))
                            :vivo/count (count results)
                            :vivo/concat (apply concat results))]
                [value expanded-path])))))))

  (<get-log [this branch limit]
    (au/go
      (let [storage (get-storage this branch)
            db-id (au/<? (<get-db-id this branch))
            dbi (au/<? (u/<get-in storage db-id u/db-info-schema nil nil))
            make-log-rec #(select-keys % [:msg :timestamp-ms])]
        (when dbi
          (loop [out [(make-log-rec dbi)]
                 i 1]
            (let [{:keys [prev-db-id]} (peek out)]
              (if (or (not prev-db-id)
                      (= limit i))
                out
                (let [prev-dbi (au/<? (u/<get-in storage prev-db-id
                                                 u/db-info-schema nil nil))]
                  (recur (conj out (make-log-rec dbi))
                         (inc i))))))))))

  (<get-num-commits [this branch]
    (au/go
      (let [storage (get-storage this branch)
            db-id (au/<? (<get-db-id this branch))
            dbi (au/<? (u/<get-in storage db-id u/db-info-schema nil nil))]
        (if dbi
          (inc (:num-prev-dbs dbi))
          0))))

  (<get-state [this arg metadata]
    ;; TODO: Figure this out for non-locally-cached state lookups
    ;; Figure out how to unify authorization-fn and redaction-fn
    #_
    (au/go
      (let [{:keys [path db-id]} arg
            {:keys [conn-id]} metadata
            {:keys [subject-id]} (@*conn-id->info conn-id)
            [v xp] (au/<? (<get-state-and-expanded-path this db-id path))
            authorized? (let [ret (authorization-fn subject-id path :read v)]
                          (if (au/channel? ret)
                            (au/<? ret)
                            ret))]
        (if (or (not authorized?)
                (= :vivo/unauthorized v))
          :vivo/unauthorized
          (when v
            (let [schema-path (rest path) ; Ignore :sys
                  schema (u/path->schema path->schema-cache state-schema
                                         schema-path)
                  fp (when schema
                       (au/<? (u/<schema->fp perm-storage schema)))]
              (when schema
                {:serialized-value {:fp fp
                                    :bytes (l/serialize schema v)}
                 :expanded-path xp})))))))

  (<get-subject-id-for-identifier [this identifier branch]
    (au/go
      (let [db-info (au/<? (<get-db-info this branch))
            id->sid-data-id (:identifier-to-subject-id-data-id db-info)
            src-storage (get-storage this id->sid-data-id)]
        (au/<? (u/<get-in src-storage id->sid-data-id
                          u/string-map-schema [identifier] nil)))))

  (<log-in* [this identifier secret minutes-valid conn-id branch]
    (<do-login this conn-id branch identifier secret
               login-identifier-case-sensitive? minutes-valid
               *conn-id->info *branch->info))

  (<log-in [this arg metadata]
    (let [{:keys [identifier secret]} arg
          {:keys [conn-id]} metadata
          {:keys [branch]} (@*conn-id->info conn-id)]
      (<log-in* this identifier secret login-lifetime-mins conn-id branch)))

  (<log-in-w-token [this token metadata]
    (<log-in-w-token* this state-schema redaction-fn vc-ep perm-storage
                      token metadata *conn-id->info))

  (<log-out [this arg metadata]
    (let [{:keys [conn-id]} metadata
          {:keys [subject-id branch]} (@*conn-id->info conn-id)]
      (<log-out-subject! this subject-id branch *branch->info *conn-id->info)))

  (<log-out-w-token [this token metadata]
    (au/go
      (let [{:keys [conn-id]} metadata
            {:keys [branch]} (@*conn-id->info conn-id)
            db-info (au/<? (<get-db-info this branch))
            {:keys [token-to-token-info-data-id]} db-info
            src-storage (get-storage this token-to-token-info-data-id)
            info (when token-to-token-info-data-id
                   (au/<? (u/<get-in src-storage token-to-token-info-data-id
                                     u/token-map-schema [token] nil)))]
        (if-not info
          false
          (let [{:keys [subject-id]} info]
            (au/<? (<log-out-subject! this subject-id branch
                                      *branch->info *conn-id->info)))))))

  (<set-state-source [this source metadata]
    (<set-state-source* this source metadata))

  (<rpc [this msg-arg metadata]
    (au/go
      (let [{:keys [conn-id]} metadata
            {:keys [subject-id branch]} (@*conn-id->info conn-id)
            db-id (au/<? (<get-db-id this branch))
            {:keys [rpc-name-kw-ns rpc-name-kw-name arg]} msg-arg
            rpc-name-kw (keyword rpc-name-kw-ns rpc-name-kw-name)
            rpc-info (rpcs rpc-name-kw)
            _ (when-not rpc-info
                (throw
                 (ex-info
                  (str "No RPC with name `" rpc-name-kw "` is registered. "
                       "Either this is a typo or you need to add `"
                       rpc-name-kw "` to the :rpcs map when "
                       "creating the Vivo server.")
                  {:known-rpcs (keys rpcs)
                   :given-rpc rpc-name-kw})))
            handler (@*rpc->handler rpc-name-kw)
            _ (when-not handler
                (throw
                 (ex-info
                  (str "No RPC handler for `" rpc-name-kw "` is registered. "
                       "Call `set-rpc-handler!` on the server instance to "
                       "set an RPC handler.")
                  {:known-rpcs (keys rpcs)
                   :given-rpc rpc-name-kw})))
            {:keys [arg-schema ret-schema]} rpc-info
            {:keys [fp bytes]} arg
            w-schema (au/<? (<fp->schema this fp conn-id))
            rpc-arg (l/deserialize arg-schema w-schema bytes)
            authorized? (let [path [:vivo/rpcs rpc-name-kw]
                              ret (authorization-fn subject-id path :call
                                                    rpc-arg)]
                          (if (au/channel? ret)
                            (au/<? ret)
                            ret))]
        (if-not authorized?
          :vivo/unauthorized
          (let [rpc-metadata (assoc (u/sym-map conn-id subject-id branch db-id
                                               repository-name)
                                    :vivo-server this)
                ret* (handler rpc-arg rpc-metadata)
                ret (if (au/channel? ret*)
                      (au/<? ret*)
                      ret*)]
            {:fp (au/<? (u/<schema->fp perm-storage ret-schema))
             :bytes (l/serialize ret-schema ret)})))))

  (<handle-request-db-changed-msg [this arg metadata]
    (au/go
      (let [{:keys [conn-id]} metadata
            {:keys [subject-id branch]} (@*conn-id->info conn-id)
            db-id (au/<? (<get-db-id this branch))
            [whole-state xp] (au/<? (<get-state-and-expanded-path
                                     this db-id [:sys]))
            new-state (when whole-state
                        (redaction-fn subject-id whole-state))
            fp (au/<? (u/<schema->fp perm-storage state-schema))
            bytes (when new-state
                    (l/serialize state-schema new-state))
            serialized-state (u/sym-map fp bytes)
            info (-> (u/sym-map db-id serialized-state subject-id)
                     (assoc :prev-db-id db-id)
                     (assoc :update-infos []))]
        (ep/send-msg vc-ep conn-id :db-changed info))))

  (<scmds->cmds [this scmds conn-id]
    (au/go
      (if-not (seq scmds)
        []
        ;; Use loop/recur to stay in single go block
        (loop [scmd (first scmds)
               i 0
               out []]
          (let [{:keys [path arg]} scmd
                arg-sch (u/path->schema path->schema-cache state-schema
                                        (rest path))  ; Ignore :sys

                new-i (inc i)
                last? (= (count scmds) new-i)]
            (if-not arg-sch ; Skip cmd if we don't have schema (evolution)
              (if last?
                out
                (recur (nth scmds new-i) new-i out))
              (let [writer-arg-sch (when arg
                                     (au/<? (<fp->schema this (:fp arg)
                                                         conn-id)))
                    arg* (when arg
                           (l/deserialize arg-sch writer-arg-sch (:bytes arg)))
                    cmd (cond-> scmd
                          arg (assoc :arg arg*))
                    new-out (conj out cmd)]
                (if last?
                  new-out
                  (recur (nth scmds new-i) new-i new-out)))))))))

  (<update-db [this update-cmds msg subject-id branch]
    (<modify-db! this (partial <update-state-update-fn
                               state-schema update-cmds)
                 "Update db" subject-id branch nil))

  (<update-state [this arg metadata]
    (<do-update-state this arg metadata authorization-fn redaction-fn
                      state-schema perm-storage *conn-id->info))

  (<get-schema-pcf [this fp metadata]
    (au/go
      (if-let [schema (au/<? (u/<fp->schema perm-storage fp))]
        (l/pcf schema)
        (do
          (log/error (str "Could not find PCF for fingerprint `" fp "`."))
          nil))))

  (<store-schema-pcf [this pcf metadata]
    (au/go
      (au/<? (u/<schema->fp perm-storage (l/json->schema pcf)))
      true))

  (get-storage [this branch-or-db-id]
    (when branch-or-db-id
      (if (str/starts-with? branch-or-db-id "-")
        temp-storage
        perm-storage)))

  (set-rpc-handler! [this rpc-name-kw handler]
    (swap! *rpc->handler assoc rpc-name-kw handler))

  (publish-msg [this arg metadata]
    (publish-msg* vc-ep *conn-id->info arg metadata))

  (shutdown! [this]
    (stop-server)))

(defn default-health-http-handler [req]
  (if (= "/health" (:uri req))
    {:status 200
     :headers {"content-type" "text/plain"
               "Access-Control-Allow-Origin" "*"}
     :body "I am healthy"}
    {:status 404
     :body "I still haven't found what you're looking for..."}))

(def default-config
  {:additional-endpoints []
   :authenticate-admin-client (constantly false)
   :handle-http default-health-http-handler
   :http-timeout-ms 60000
   :login-lifetime-mins (* 60 24 15)  ;; 15 days
   :redaction-fn (fn [subject-id db]
                   db)})

(defn check-config [config]
  (let [required-ks [:authorization-fn :port :repository-name :state-schema]]
    (doseq [k required-ks]
      (when-not (config k)
        (throw (ex-info (str "Missing " k " in config.")
                        (u/sym-map k config))))))
  (when-let [rpcs (:rpcs config)]
    (when-not (map? rpcs)
      (throw (ex-info (str "In config, the value of the :rpcs key "
                           "must be a map. Got `" rpcs "`.")
                      config)))
    (u/check-rpcs rpcs)))

(defn on-disconnect
  [*conn-id->info *branch->info temp-storage tube-conn code reason conn-count]
  (ca/go
    (let [conn-id (when tube-conn (tc/get-conn-id tube-conn))]
      (try
        (let [conn-info (@*conn-id->info conn-id)
              {:keys [branch subject-id temp-branch?]} conn-info]
          (swap! *conn-id->info dissoc conn-id)
          (swap! *branch->info update branch
                 (fn [info]
                   (-> info
                       (update :conn-ids disj conn-id)
                       (update :subject-id->conn-ids dissoc subject-id))))
          (when temp-branch?
            (au/<? (<delete-branch* branch temp-storage))))
        (catch Throwable e
          (log/error (str "Error in on-disconnect (conn-id: " conn-id ")\n"
                          (u/ex-msg-and-stacktrace e))))))))

(defn <get-subject-id-for-identifier* [vivo-server identifier metadata]
  (let [{:keys [branch]} (@(:*conn-id->info vivo-server) (:conn-id metadata))]
    (<get-subject-id-for-identifier vivo-server identifier branch)))

(defn set-handlers! [vivo-server vc-ep admin-ep]
  (ep/set-handler admin-ep :create-branch (partial <create-branch vivo-server))
  (ep/set-handler admin-ep :delete-branch (partial <delete-branch vivo-server))
  (ep/set-handler admin-ep :get-db-id-for-branch
                  (fn [arg metadata]
                    (<get-db-id vivo-server arg)))
  (ep/set-handler vc-ep :add-subject (partial <add-subject vivo-server))
  (ep/set-handler vc-ep :add-subject-identifier
                  (partial <add-subject-identifier vivo-server))
  (ep/set-handler vc-ep :change-secret (partial <change-secret vivo-server))
  (ep/set-handler vc-ep :get-schema-pcf (partial <get-schema-pcf vivo-server))
  (ep/set-handler vc-ep :get-subject-id-for-identifier
                  (partial <get-subject-id-for-identifier* vivo-server))
  (ep/set-handler vc-ep :get-state (partial <get-state vivo-server))
  (ep/set-handler vc-ep :log-in (partial <log-in vivo-server))
  (ep/set-handler vc-ep :log-in-w-token (partial <log-in-w-token vivo-server))
  (ep/set-handler vc-ep :log-out (partial <log-out vivo-server))
  (ep/set-handler vc-ep :log-out-w-token (partial <log-out-w-token vivo-server))
  (ep/set-handler vc-ep :publish-msg
                  (partial publish-msg vivo-server))
  (ep/set-handler vc-ep :remove-subject-identifier
                  (partial <remove-subject-identifier vivo-server))
  (ep/set-handler vc-ep :request-db-changed-msg
                  (partial <handle-request-db-changed-msg vivo-server))
  (ep/set-handler vc-ep :rpc (partial <rpc vivo-server))
  (ep/set-handler vc-ep :set-state-source
                  (partial <set-state-source vivo-server))
  (ep/set-handler vc-ep :store-schema-pcf
                  (partial <store-schema-pcf vivo-server))
  (ep/set-handler vc-ep :update-state (partial <update-state vivo-server)))

(defn vivo-server
  [config]
  (when-not (map? config)
    (throw (ex-info (str "`config` argument must be a map. Got: `" config "`.")
                    (u/sym-map config))))
  (let [config* (merge default-config config)
        _ (check-config config*)
        {:keys [additional-endpoints
                authenticate-admin-client
                authorization-fn
                certificate-str
                disable-ddb?
                handle-http
                http-timeout-ms
                login-identifier-case-sensitive?
                login-lifetime-mins
                port
                private-key-str
                redaction-fn
                repository-name
                rpcs
                s3-data-storage-bucket
                state-schema]} config*
        perm-storage (data-storage/data-storage
                      (data-block-storage/data-block-storage
                       (if disable-ddb?
                         (mem-block-storage/mem-block-storage false)
                         (au/<?? (ddb-block-storage/<ddb-block-storage
                                  repository-name))))
                      s3-data-storage-bucket)
        temp-storage (data-storage/data-storage
                      (data-block-storage/data-block-storage
                       (mem-block-storage/mem-block-storage true))
                      nil)
        modify-q (ConcurrentLinkedQueue.)
        path->schema-cache (sr/stockroom 1000)
        *conn-id->info (atom {})
        *branch->info (atom {})
        *rpc->handler (atom {})
        vc-ep-opts {:on-disconnect (partial on-disconnect *conn-id->info
                                            *branch->info
                                            temp-storage)}
        vc-ep (ep/endpoint "vivo-client" (constantly true)
                           u/client-server-protocol :server vc-ep-opts)
        admin-ep (ep/endpoint "admin-client" authenticate-admin-client
                              u/admin-client-server-protocol :server)
        cs-opts (u/sym-map handle-http http-timeout-ms
                           certificate-str private-key-str)
        stop-server (cs/server (conj additional-endpoints vc-ep admin-ep)
                               port cs-opts)
        vivo-server (->VivoServer authorization-fn
                                  login-identifier-case-sensitive?
                                  login-lifetime-mins
                                  modify-q
                                  path->schema-cache
                                  perm-storage
                                  (or rpcs {})
                                  vc-ep
                                  redaction-fn
                                  repository-name
                                  state-schema
                                  stop-server
                                  temp-storage
                                  *conn-id->info
                                  *branch->info
                                  *rpc->handler)]
    (au/<?? (<store-schema-pcf vivo-server (l/pcf state-schema) nil))
    (start-modify-db-loop modify-q redaction-fn state-schema vc-ep temp-storage
                          perm-storage *branch->info *conn-id->info)
    (set-handlers! vivo-server vc-ep admin-ep)
    (log/info (str "Vivo server started on port " port "."))
    vivo-server))
