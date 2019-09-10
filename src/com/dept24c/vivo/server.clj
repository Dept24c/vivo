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
   [com.dept24c.vivo.utils :as u]
   [crypto.password.bcrypt :as bcrypt]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.endpoint :as ep]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr])
  (:import
   (clojure.lang ExceptionInfo)
   (java.security SecureRandom)
   (java.util UUID)))

(set! *warn-on-reflection* true)

(def work-factor 12)

(defprotocol IVivoServer
  (<add-subject [this arg metadata])
  (<add-subject-identifier [this arg metadata])
  (<change-secret [this arg metadata])
  (<create-branch [this arg metadata])
  (<delete-branch [this branch metadata])
  (<fp->schema [this fp conn-id])
  (<get-all-branches [this])
  (<get-db-id [this branch])
  (<get-in [this db-id path])
  (<get-log [this branch limit])
  (<get-num-commits [this branch])
  (<get-schema-pcf [this arg metadata])
  (<get-state [this arg metadata])
  (<log-in [this arg metadata])
  (<log-in-w-token [this arg metadata])
  (<log-out [this arg metadata])
  (<modify-db [this <update-fn msg updated-paths metadata])
  (<set-state-source [this arg metadata])
  (<store-schema-pcf [this arg metadata])
  (<update-state [this arg metadata])
  (<scmds->cmds [this scmds conn-id])
  (get-storage [this branch]))

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

(defn <add-subject-update-fn
  [requested-subject-id identifier hashed-secret dbi storage subject-id]
  (au/go
    (let [{:keys [identifier-to-subject-id-data-id
                  subject-id-to-hashed-secret-data-id]} dbi]
      (assoc dbi
             :subject-id-to-hashed-secret-data-id
             (au/<? (u/<update storage
                               subject-id-to-hashed-secret-data-id
                               u/string-map-schema
                               [{:path [requested-subject-id]
                                 :op :set
                                 :arg hashed-secret}]))
             :identifier-to-subject-id-data-id
             (au/<? (u/<update storage
                               identifier-to-subject-id-data-id
                               u/string-map-schema
                               [{:path [identifier]
                                 :op :set
                                 :arg requested-subject-id}]))))))

(defn <add-subject-identifier-update-fn [identifier dbi storage subject-id]
  (au/go
    (let [{:keys [identifier-to-subject-id-data-id]} dbi]
      (assoc dbi
             :identifier-to-subject-id-data-id
             (au/<? (u/<update storage
                               identifier-to-subject-id-data-id
                               u/string-map-schema
                               [{:path [identifier]
                                 :op :set
                                 :arg subject-id}]))))))

(defn <change-secret-update-fn [hashed-secret dbi storage subject-id]
  (au/go
    (let [{:keys [subject-id-to-hashed-secret-data-id]} dbi]
      (assoc dbi
             :subject-id-to-hashed-secret-data-id
             (au/<? (u/<update storage
                               subject-id-to-hashed-secret-data-id
                               u/string-map-schema
                               [{:path [subject-id]
                                 :op :set
                                 :arg hashed-secret}]))))))

(defn <log-in-update-fn [token token-info dbi storage subject-id]
  (au/go
    (let [{:keys [token-to-token-info-data-id
                  subject-id-to-tokens-data-id]} dbi]
      (assoc dbi
             :token-to-token-info-data-id
             (au/<? (u/<update storage
                               token-to-token-info-data-id
                               u/token-map-schema
                               [{:path [token]
                                 :op :set
                                 :arg token-info}]))
             :subject-id-to-tokens-data-id
             (au/<? (u/<update storage
                               subject-id-to-tokens-data-id
                               u/subject-id-to-tokens-schema
                               [{:path [subject-id -1]
                                 :op :insert-after
                                 :arg token}]))))))

(defn <log-out-update-fn [dbi storage subject-id]
  (au/go
    (let [{:keys [token-to-token-info-data-id
                  subject-id-to-tokens-data-id]} dbi
          tokens (au/<? (u/<get-in storage subject-id-to-tokens-data-id
                                   u/subject-id-to-tokens-schema
                                   [subject-id]))
          dbi* (assoc dbi
                      :subject-id-to-tokens-data-id
                      (au/<? (u/<update storage
                                        subject-id-to-tokens-data-id
                                        u/subject-id-to-tokens-schema
                                        [{:path [subject-id]
                                          :op :remove}])))]
      ;; Use loop here to stay in go block
      (loop [new-dbi dbi*
             [token & more] tokens]
        (let [new-dbi* (assoc new-dbi
                              :token-to-token-info-data-id
                              (au/<? (u/<update storage
                                                token-to-token-info-data-id
                                                u/token-map-schema
                                                [{:path [token]
                                                  :op :remove}])))]
          (if (seq more)
            (recur new-dbi* more)
            new-dbi*))))))

(defn <delete-token-update-fn [token dbi storage subject-id]
  (au/go
    (let [{:keys [token-to-token-info-data-id
                  subject-id-to-tokens-data-id]} dbi
          tokens (au/<? (u/<get-in storage subject-id-to-tokens-data-id
                                   u/subject-id-to-tokens-schema
                                   [subject-id]))
          new-tokens (-> (set tokens)
                         (disj token))]
      (assoc dbi
             :token-to-token-info-data-id
             (au/<? (u/<update storage
                               token-to-token-info-data-id
                               u/token-map-schema
                               [{:path [token]
                                 :op :remove}]))
             ;; TODO: Improve this when `find` is implemented
             :subject-id-to-tokens-data-id
             (au/<? (u/<update storage
                               subject-id-to-tokens-data-id
                               u/subject-id-to-tokens-schema
                               [{:path [subject-id]
                                 :op :set
                                 :arg new-tokens}]))))))

(defn <update-state-update-fn
  [state-schema update-cmds tx-fns dbi storage subject-id]
  (au/go
    (let [{:keys [data-id]} dbi]
      (assoc dbi :data-id (au/<? (u/<update storage data-id state-schema
                                            update-cmds tx-fns))))))

(defn <delete-branch* [branch storage]
  (au/go
    (let [all-branches (au/<? (u/<get-in-reference
                               storage u/all-branches-reference
                               u/all-branches-schema nil))
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
                                     :arg new-branches}]))
      (au/<? (u/<delete-reference! storage branch-reference)))))

(defrecord VivoServer [admin-secret
                       authorization-fn
                       log-error
                       log-info
                       login-lifetime-mins
                       path->schema-cache
                       perm-storage
                       sm-ep
                       state-schema
                       temp-storage
                       tx-fns
                       *conn-id->info
                       *subject-id->conn-ids
                       *branch->info]
  IVivoServer
  (<modify-db [this <update-fn msg updated-paths metadata]
    ;; TODO: Do these serially in a queue to minimize conflicts
    (au/go
      (let [{:keys [conn-id]} metadata
            {:keys [subject-id branch]} (@*conn-id->info conn-id)
            {:keys [conn-ids]} (@*branch->info branch)
            num-tries 100
            branch-reference (branch->reference branch)]
        (loop [num-tries-left (dec num-tries)]
          (let [storage (get-storage this branch)
                prev-db-id (au/<? (u/<get-data-id storage branch-reference))
                prev-dbi (when prev-db-id
                           (au/<? (u/<get-in storage prev-db-id
                                             u/db-info-schema nil)))
                {:keys [num-prev-dbs data-id]} prev-dbi
                cur-dbi (-> (<update-fn prev-dbi storage subject-id)
                            (au/<?)
                            (assoc :msg msg
                                   :timestamp-ms (u/current-time-ms)
                                   :num-prev-dbs (if num-prev-dbs
                                                   (inc num-prev-dbs)
                                                   0)
                                   :prev-db-id prev-db-id))
                cur-db-id (au/<? (u/<update storage nil u/db-info-schema
                                            [{:path nil
                                              :op :set
                                              :arg cur-dbi}]))]
            (if (au/<? (u/<compare-and-set! storage branch-reference
                                            l/string-schema
                                            prev-db-id cur-db-id))
              (let [change-info (u/sym-map cur-db-id prev-db-id updated-paths)]
                (doseq [conn-id* (disj (set conn-ids) conn-id)]
                  (ep/send-msg sm-ep conn-id* :sys-state-changed
                               change-info))
                change-info)
              (if (zero? num-tries-left)
                (throw (ex-info (str "Failed to commit to branch `" branch
                                     "` after " num-tries " tries.")
                                (u/sym-map branch num-tries)))
                (do
                  (au/<? (ca/timeout (rand-int 100)))
                  (recur (dec num-tries-left))))))))))

  (<add-subject [this arg metadata]
    (au/go
      (let [{:keys [identifier secret subject-id]
             :or {subject-id (.toString ^UUID (UUID/randomUUID))}} arg
            hashed-secret (bcrypt/encrypt secret work-factor)]
        (au/<? (<modify-db this (partial <add-subject-update-fn subject-id
                                         identifier hashed-secret)
                           (str "Add subject " subject-id)
                           [:vivo/authentication] metadata))
        subject-id)))

  (<add-subject-identifier [this identifier metadata]
    (<modify-db this (partial <add-subject-identifier-update-fn identifier)
                (str "Add subject indentifier `" identifier)
                [:vivo/authentication] metadata)
    true)

  (<change-secret [this new-secret metadata]
    (let [hashed-secret (bcrypt/encrypt new-secret work-factor)]
      (<modify-db this (partial <change-secret-update-fn hashed-secret)
                  "Change secret"
                  [:vivo/authentication] metadata)))

  (<create-branch [this arg metadata]
    (au/go
      (let [{:keys [branch db-id]
             temp-dest? :is-temp} arg
            _ (when (> (count branch) u/max-branch-name-len)
                (throw-branch-name-too-long branch))
            _ (when (and temp-dest?
                         (not (str/starts-with? branch "-")))
                (throw (ex-info (str "Temp branch names must start with `-`. "
                                     "Got `" branch "`.")
                                (u/sym-map branch))))
            temp-src? (and db-id (block-ids/temp-block-id? db-id))
            storage (get-storage this branch)
            db-id* (if temp-dest?
                     (block-ids/block-id->temp-block-id db-id)
                     db-id)
            branch-reference (branch->reference branch)]
        (when (and temp-src? (not temp-dest?))
          (throw (ex-info (str "Cannot create a permanent branch from a "
                               "temporary db-id.")
                          (u/sym-map branch db-id temp-dest?))))
        (when (au/<? (u/<get-data-id storage branch-reference))
          (throw (ex-info (str "A branch named `" branch "` already "
                               "exists in the repository.")
                          (u/sym-map branch db-id temp-dest?))))
        (au/<? (u/<update-reference! storage u/all-branches-reference
                                     u/all-branches-schema
                                     [{:path [-1]
                                       :op :insert-after
                                       :arg branch}]))
        true)))

  (<delete-branch [this branch metadata]
    (<delete-branch* branch (get-storage this branch)))

  (<fp->schema [this fp conn-id]
    (au/go
      (or (au/<? (u/<fp->schema perm-storage fp))
          (let [pcf (au/<? (ep/<send-msg sm-ep conn-id
                                         :get-schema-pcf fp))]
            (l/json->schema pcf)))))

  (<get-all-branches [this]
    (au/go
      (let [perm-branches (au/<? (u/<get-in-reference
                                  perm-storage u/all-branches-reference
                                  u/all-branches-schema nil))
            temp-branches (au/<? (u/<get-in-reference
                                  temp-storage u/all-branches-reference
                                  u/all-branches-schema nil))
            all-branches (concat perm-branches temp-branches)]
        (when (seq all-branches)
          all-branches))))

  (<get-db-id [this branch]
    (au/go
      (let [storage (get-storage this branch)
            branch-reference (branch->reference branch)]
        (au/<? (u/<get-data-id storage branch-reference)))))

  (<get-in [this db-id path]
    (au/go
      (when-not (string? db-id)
        (throw (ex-info (str "Bad db-id `" db-id
                             "`. db-id must be a string.")
                        {:db-id db-id})))
      (when (and path (not (sequential? path)))
        (throw (ex-info (str "Bad path `" path
                             "`. Path must be nil or a sequence.")
                        {:path path})))
      (let [path* (or path [])
            storage (if (block-ids/temp-block-id? db-id)
                      temp-storage
                      perm-storage)
            data-id (au/<? (u/<get-in storage db-id u/db-info-schema
                                      [:data-id]))]
        (au/<? (u/<get-in storage data-id state-schema path*)))))

  (<get-log [this branch limit]
    (au/go
      (let [storage (get-storage this branch)
            db-id (au/<? (<get-db-id this branch))
            dbi (au/<? (u/<get-in storage db-id u/db-info-schema nil))
            make-log-rec #(select-keys % [:msg :timestamp-ms])]
        (when dbi
          (loop [out [(make-log-rec dbi)]
                 i 1]
            (let [{:keys [prev-db-id]} (peek out)]
              (if (or (not prev-db-id)
                      (= limit i))
                out
                (let [prev-dbi (au/<? (u/<get-in storage prev-db-id
                                                 u/db-info-schema nil))]
                  (recur (conj out (make-log-rec dbi))
                         (inc i))))))))))

  (<get-num-commits [this branch]
    (au/go
      (let [storage (get-storage this branch)
            db-id (au/<? (<get-db-id this branch))
            dbi (au/<? (u/<get-in storage db-id u/db-info-schema nil))]
        (if dbi
          (inc (:num-prev-dbs dbi))
          0))))

  (<get-state [this arg metadata]
    (au/go
      (let [{:keys [path db-id]} arg
            {:keys [conn-id]} metadata
            {:keys [subject-id branch]} (@*conn-id->info conn-id)
            v (au/<? (<get-in this db-id path))
            authorized? (let [ret (authorization-fn subject-id path :read v)]
                          (if (au/channel? ret)
                            (au/<? ret)
                            ret))]
        (if-not authorized?
          :vivo/unauthorized
          (when v
            (let [schema (or (sr/get path->schema-cache path)
                             (let [sch (l/schema-at-path state-schema path)]
                               (sr/put! path->schema-cache path sch)
                               sch))
                  fp (au/<? (u/<schema->fp perm-storage schema))]
              {:fp fp
               :bytes (l/serialize schema v)}))))))

  (<log-in [this arg metadata]
    (au/go
      (let [{:keys [identifier secret]} arg
            {:keys [conn-id]} metadata
            {:keys [branch]} (@*conn-id->info conn-id)
            branch-reference (branch->reference branch)
            storage (get-storage this branch)
            db-id (au/<? (u/<get-data-id storage branch-reference))
            db-info (au/<? (u/<get-in storage db-id u/db-info-schema nil))
            {id->sid-data-id :identifier-to-subject-id-data-id
             sid->hs-data-id :subject-id-to-hashed-secret-data-id} db-info
            subject-id (au/<? (u/<get-in storage id->sid-data-id
                                         u/string-map-schema
                                         [identifier]))
            hashed-secret (when subject-id
                            (au/<? (u/<get-in storage sid->hs-data-id
                                              u/string-map-schema
                                              [subject-id])))]
        (when (and hashed-secret
                   (bcrypt/check secret hashed-secret))
          (let [token (generate-token)
                expiration-time-mins (+ (u/ms->mins (u/current-time-ms))
                                        login-lifetime-mins)
                token-info (u/sym-map expiration-time-mins subject-id)]
            (swap! *conn-id->info update conn-id assoc :subject-id subject-id)
            (swap! *subject-id->conn-ids update subject-id
                   (fn [conn-ids]
                     (conj (or conn-ids #{}) conn-id)))
            (au/<? (<modify-db this (partial <log-in-update-fn
                                             token token-info)
                               "Log in" [:vivo/authentication] metadata))
            (u/sym-map subject-id token))))))

  (<log-in-w-token [this token metadata]
    (au/go
      (let [{:keys [conn-id]} metadata
            {:keys [branch]} (@*conn-id->info conn-id)
            branch-reference (branch->reference branch)
            storage (get-storage this branch)
            db-id (au/<? (u/<get-data-id storage branch-reference))
            db-info (au/<? (u/<get-in storage db-id u/db-info-schema nil))
            {:keys [token-to-token-info-data-id]} db-info
            info (when token-to-token-info-data-id
                   (au/<? (u/<get-in storage token-to-token-info-data-id
                                     u/token-map-schema
                                     [token])))
            {:keys [expiration-time-mins subject-id]} info
            now-mins (-> (u/current-time-ms)
                         (u/ms->mins))]
        (when info
          (if (>= now-mins expiration-time-mins)
            (do
              (au/<? (<modify-db this (partial <delete-token-update-fn token)
                                 "Delete expired token"
                                 [:vivo/authentication] metadata))
              nil)
            (do
              (swap! *conn-id->info update conn-id assoc :subject-id subject-id)
              (swap! *subject-id->conn-ids update subject-id
                     (fn [conn-ids]
                       (conj (or conn-ids #{}) conn-id)))
              subject-id))))))

  (<log-out [this arg metadata]
    (au/go
      (let [{:keys [conn-id]} metadata
            {:keys [subject-id]} (@*conn-id->info conn-id)
            conn-ids (@*subject-id->conn-ids subject-id)]
        (au/<? (<modify-db this <log-out-update-fn
                           "Log out" [:vivo/authentication] metadata))
        (doseq [conn-id conn-ids]
          (ep/close-conn sm-ep conn-id)))))

  (<set-state-source [this source metadata]
    (au/go
      (let [{:keys [conn-id]} metadata
            perm-branch (:branch/name source)
            branch (or perm-branch
                       (let [branch* (str "-temp-branch-" (rand-int 1e9))]
                         (au/<? (<create-branch
                                 this {:branch branch*
                                       :db-id (:temp-branch/db-id source)
                                       :is-temp true}
                                 metadata))
                         branch*))]
        (swap! *conn-id->info update conn-id assoc
               :branch branch :temp-branch? (not perm-branch))
        (swap! *branch->info update branch
               (fn [{:keys [conn-ids] :as info}]
                 (if conn-ids
                   (update info :conn-ids conj conn-id)
                   (assoc info :conn-ids #{conn-id}))))
        (or (au/<? (<get-db-id this branch))
            (let [default-data (l/default-data state-schema)
                  update-cmds [{:path []
                                :op :set
                                :arg default-data}]]
              (-> (<modify-db this (partial <update-state-update-fn state-schema
                                            update-cmds tx-fns)
                              "Create initial db" [] metadata)
                  (au/<?)
                  (:cur-db-id)))))))

  (<scmds->cmds [this scmds conn-id]
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
                              (let [sch (l/schema-at-path state-schema path)]
                                (sr/put! path->schema-cache path sch)
                                sch)))
                writer-arg-sch (when arg
                                 (au/<? (<fp->schema this (:fp arg)
                                                     conn-id)))
                arg* (when arg
                       (l/deserialize arg-sch writer-arg-sch (:bytes arg)))
                cmd (cond-> scmd
                      arg (assoc :arg arg*))
                new-i (inc i)
                new-out (conj out cmd)]
            (if (> (count scmds) new-i)
              (recur (nth scmds new-i) new-i new-out)
              new-out))))))

  (<update-state [this arg metadata]
    ;; TODO: Use authorization here
    (au/go
      (let [{:keys [conn-id]} metadata
            update-cmds (au/<? (<scmds->cmds this arg conn-id))
            updated-paths (map :path update-cmds)]
        (au/<? (<modify-db this (partial <update-state-update-fn
                                         state-schema update-cmds tx-fns)
                           "Update state" updated-paths metadata)))))

  (<get-schema-pcf [this fp metadata]
    (au/go
      (if-let [schema (au/<? (u/<fp->schema perm-storage fp))]
        (l/pcf schema)
        (do
          (log-error (str "Could not find PCF for fingerprint `" fp "`."))
          nil))))

  (<store-schema-pcf [this pcf metadata]
    (au/go
      (au/<? (u/<schema->fp perm-storage (l/json->schema pcf)))
      true))

  (get-storage [this branch]
    (if (str/starts-with? branch "-")
      temp-storage
      perm-storage)))

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
   :handle-http default-health-http-handler
   :http-timeout-ms 60000
   :log-info println
   :log-error println ;; TODO: use stderr
   :login-lifetime-mins (* 60 24 15)}) ;; 15 days

(defn check-config [config]
  (let [required-ks [:authorization-fn :port :repository-name :state-schema]]
    (doseq [k required-ks]
      (when-not (config k)
        (throw (ex-info (str "Missing " k " in config.")
                        (u/sym-map k config)))))))

(defn on-disconnect
  [*conn-id->info *subject-id->conn-ids *branch->info temp-storage
   log-error log-info {:keys [conn-id] :as conn-info}]
  (ca/go
    (try
      (let [{:keys [branch subject-id temp-branch?]} (@*conn-id->info conn-id)]
        (swap! *conn-id->info dissoc conn-id)
        (swap! *branch->info update branch update :conn-ids disj conn-id)
        (swap! *subject-id->conn-ids
               (fn [m]
                 (let [new-conn-ids (disj (m subject-id) conn-id)]
                   (if (seq new-conn-ids)
                     (assoc m subject-id new-conn-ids)
                     (dissoc m subject-id)))))
        (when temp-branch?
          (au/<? (<delete-branch* branch temp-storage)))
        (log-info "Client disconnected (conn-info: " conn-info ")."))
      (catch Throwable e
        (log-error (str "Error in on-disconnect (conn-info: " conn-info ")\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn vivo-server
  "Returns a no-arg fn that stops the server."
  [config]
  (let [config* (merge default-config config)
        _ (check-config config*)
        {:keys [additional-endpoints
                admin-secret
                authorization-fn
                handle-http
                http-timeout-ms
                log-error
                log-info
                login-lifetime-mins
                port
                repository-name
                state-schema
                tx-fns]} config*
        perm-storage (data-storage/data-storage
                      (data-block-storage/data-block-storage
                       (au/<?? (ddb-block-storage/<ddb-block-storage
                                repository-name))))
        temp-storage (data-storage/data-storage
                      (data-block-storage/data-block-storage
                       (mem-block-storage/mem-block-storage true)))
        path->schema-cache (sr/stockroom 1000)
        *conn-id->info (atom {})
        *subject-id->conn-ids (atom {})
        *branch->info (atom {})
        sm-ep-opts {:on-disconnect (partial on-disconnect *conn-id->info
                                            *subject-id->conn-ids *branch->info
                                            temp-storage log-error log-info)}
        sm-ep (ep/endpoint "vivo-client" (constantly true)
                           u/client-server-protocol :server sm-ep-opts)
        cs-opts (u/sym-map handle-http http-timeout-ms)
        capsule-server (cs/server (conj additional-endpoints sm-ep)
                                  port cs-opts)
        vivo-server (->VivoServer admin-secret
                                  authorization-fn
                                  log-error
                                  log-info
                                  login-lifetime-mins
                                  path->schema-cache
                                  perm-storage
                                  sm-ep
                                  state-schema
                                  temp-storage
                                  tx-fns
                                  *conn-id->info
                                  *subject-id->conn-ids
                                  *branch->info)]
    (ep/set-handler sm-ep :add-subject
                    (partial <add-subject vivo-server))
    (ep/set-handler sm-ep :add-subject-identifier
                    (partial <add-subject-identifier vivo-server))
    (ep/set-handler sm-ep :change-secret
                    (partial <change-secret vivo-server))
    (ep/set-handler sm-ep :get-schema-pcf
                    (partial <get-schema-pcf vivo-server))
    (ep/set-handler sm-ep :store-schema-pcf
                    (partial <store-schema-pcf vivo-server))
    (ep/set-handler sm-ep :get-state
                    (partial <get-state vivo-server))
    (ep/set-handler sm-ep :log-in
                    (partial <log-in vivo-server))
    (ep/set-handler sm-ep :log-in-w-token
                    (partial <log-in-w-token vivo-server))
    (ep/set-handler sm-ep :log-out
                    (partial <log-out vivo-server))
    (ep/set-handler sm-ep :set-state-source
                    (partial <set-state-source vivo-server))
    (ep/set-handler sm-ep :update-state
                    (partial <update-state vivo-server))
    (cs/start capsule-server)
    (log-info (str "Vivo server started on port " port "."))
    #(cs/stop capsule-server)))
