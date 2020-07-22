(ns com.dept24c.vivo.client.topic-subscriptions
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as log]))

(defn subscribe-to-topic!
  [scope topic-name cb *next-topic-sub-id *topic-name->sub-id->cb]
  (let [sub-id (swap! *next-topic-sub-id inc)
        unsub! (fn unsubscribe! []
                 (swap! *topic-name->sub-id->cb
                        (fn [old-topic-name->sub-id->cb]
                          (let [old-sub-id->cb (old-topic-name->sub-id->cb
                                                topic-name)
                                new-sub-id->cb (dissoc old-sub-id->cb sub-id)]
                            (if (seq new-sub-id->cb)
                              (assoc old-topic-name->sub-id->cb topic-name
                                     new-sub-id->cb)
                              (dissoc old-topic-name->sub-id->cb topic-name)))))
                 true)]
    (swap! *topic-name->sub-id->cb assoc-in [topic-name sub-id] cb)
    unsub!))

(defn publish-to-topic! [scope topic-name msg *topic-name->sub-id->cb]
  (when-not (u/topic-scopes scope)
    (throw (ex-info (str "`scope` argument to `publish-to-topic!` must "
                         "be one of " u/topic-scopes ". Got `" scope "`.")
                    (u/sym-map scope topic-name msg))))
  (when-not (string? topic-name)
    (throw (ex-info (str "`topic-name` argument to `publish-to-topic!` must "
                         "be a string. Got `" topic-name "`.")
                    (u/sym-map scope topic-name msg))))
  (when (= :sys scope)
    (throw (ex-info "`publish-to-topic` doesn't support :sys scope yet."
                    (u/sym-map scope topic-name msg))))
  (case scope
    :local
    (ca/go
      (try
        (doseq [[sub-id cb] (@*topic-name->sub-id->cb topic-name)]
          (cb msg))
        (catch #?(:cljs js/Error :clj Throwable) e
          (log/error (str "Error while distributing messages:\n"
                          (u/ex-msg-and-stacktrace e)))))))
  nil)
