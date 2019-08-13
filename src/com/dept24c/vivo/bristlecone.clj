(ns com.dept24c.vivo.bristlecone
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.bristlecone.client :as client]
   [com.dept24c.vivo.bristlecone.db-ids :as db-ids]
   [com.dept24c.vivo.utils :as u])
  (:refer-clojure :exclude [get-in]))

(set! *warn-on-reflection* true)

(defn create-branch!
  "Creates a branch from the given db-id. If db-id is nil, creates an
   empty branch. If temp? is true, creates a temporary branch.
   Calls the given callback with either the result or an exception object."
  ([client branch-name db-id cb]
   (create-branch! client branch-name db-id false cb))
  ([client branch-name db-id temp? cb]
   (ca/go
     (-> (u/<create-branch! client branch-name db-id temp?)
         (ca/<!)
         (cb)))))

(defn <create-branch!
  "Creates a branch from the given db-id. If db-id is nil, creates an
   empty branch. If temp? is true, creates a temporary branch.
   Returns a channel which will yield the result or an exception object."
  ([client branch-name db-id]
   (<create-branch! client branch-name db-id false))
  ([client branch-name db-id temp?]
   (u/<create-branch! client branch-name db-id temp?)))

(defn delete-branch!
  "Deletes a branch.
   Calls the given callback with either the result or an exception object."
  [client branch-name cb]
  (ca/go
    (-> (u/<delete-branch! client branch-name)
        (ca/<!)
        (cb))))

(defn <delete-branch!
  "Deletes a branch.
   Returns a channel which will yield the result or an exception object."
  [client branch-name]
  (u/<delete-branch! client branch-name))

(defn get-in
  "Gets the value at the given path in the given db.
   Calls the given callback with either the result or an exception object."
  [client db-id path cb]
  (ca/go
    (-> (u/<get-in client db-id path)
        (ca/<!)
        (cb))))

(defn <get-in
  "Gets the value at the given path in the given db.
   Returns a channel which will yield the result or an exception object."
  [client db-id path]
  (u/<get-in client db-id path))

(defn get-all-branches
  "Gets the names of all the non-temporaray branches in the repository.
   Calls the given callback with either the result or an exception object."
  [client cb]
  (ca/go
    (-> (u/<get-all-branches client)
        (ca/<!)
        (cb))))

(defn <get-all-branches
  "Get the names of all the branches in the repository.
   Returns a channel which will yield the result or an exception object."
  [client]
  (u/<get-all-branches client))

(defn get-db-id
  "Get the db-id of the last db committed to the given branch.
   Calls the given callback with either the result or an exception object."
  [client branch cb]
  (ca/go
    (-> (u/<get-db-id client branch)
        (ca/<!)
        (cb))))

(defn <get-db-id
  "Get the db-id of the last db committed to the given branch.
   Returns a channel which will yield the result or an exception object."
  [client branch]
  (u/<get-db-id client branch))

(defn get-num-commits
  "Get the number of commits made to this branch (including any parent
   branches).
   Calls the given callback with either the result or an exception object."
  [client branch cb]
  (ca/go
    (-> (u/<get-num-commits client branch)
        (ca/<!)
        (cb))))

(defn <get-num-commits
  "Get the number of commits made to this branch (including any parent
   branches).
   Returns a channel which will yield the result or an exception object."
  [client branch]
  (u/<get-num-commits client branch))

(defn get-log
  "Get the log records for commits to this branch. Optionally limit the
   number of results.
   Calls the given callback with either the result or an exception object."
  ([client branch cb]
   (get-log client branch nil cb))
  ([client branch limit cb]
   (ca/go
     (-> (u/<get-log client branch limit)
         (ca/<!)
         (cb)))))

(defn <get-log
  "Get the log records for commits to this branch. Optionally limit the
   number of results.
   Returns a channel which will yield the result or an exception object."
  ([client branch]
   (<get-log client branch nil))
  ([client branch limit]
   (u/<get-log client branch limit)))

(defn merge!
  "Merge the source branch into the target branch.
   Calls the given callback with either the result or an exception object."
  [client source-branch target-branch cb]
  (ca/go
    (-> (u/<merge! client source-branch target-branch)
        (ca/<!)
        (cb))))

(defn <merge!
  "Merge the source branch into the target branch.
   Returns a channel which will yield the result or an exception object."
  [client source-branch target-branch]
  (u/<merge! client source-branch target-branch))

(defn commit!
  "Applies the given update commands to the given branch, creating a new db.
   Calls the given callback with either a map with keys :cur-db-id and
   :prev-db-id or an exception object."
  ([client branch update-commands msg cb]
   (commit! client branch update-commands msg [] cb))
  ([client branch update-commands msg tx-fns cb]
   (ca/go
     (-> (u/<commit! client branch update-commands msg tx-fns)
         (ca/<!)
         (cb)))))

(defn <commit!
  "Applies the given update commands to the given branch, creating a new db.
   Returns a channel which will yield either a map with keys :cur-db-id
   and :prev-db-id or an exception object."
  ([client branch update-commands msg]
   (<commit! client branch update-commands msg []))
  ([client branch update-commands msg tx-fns]
   (u/<commit! client branch update-commands msg tx-fns)))

(defn earlier?
  "Return true if db-id1 is earlier in time than db-id2, otherwise false.
   The given db-ids must be of the same type, either permanent or temporary."
  [db-id1 db-id2]
  (db-ids/earlier? db-id1 db-id2))

(defn bristlecone-client
  "Creates a bristlecone client attached to the given repository.
   Calls the given callback with either the client or an exception object."
  [repository-name storage-schema cb]
  (ca/go
    (-> (client/<bristlecone-client repository-name storage-schema)
        (ca/<!)
        (cb))))

(defn <bristlecone-client
  "Creates a bristlecone client attached to the given repository.
   Returns a channel which will yield the client or an exception object."
  [repository-name storage-schema]
  (client/<bristlecone-client repository-name storage-schema))
