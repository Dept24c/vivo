(ns com.dept24c.integration.branch-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [com.dept24c.vivo :as vivo]
   [com.dept24c.vivo.admin-client :as ac]
   [com.dept24c.vivo.state-schema :as ss]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as log]
   [deercreeklabs.lancaster :as l])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(defn make-get-server-url [ep]
  (constantly (str "ws://localhost:12345/" ep)))

(def vc-opts {:get-server-url (make-get-server-url "vivo-client")
              :rpcs ss/rpcs
              :sys-state-schema ss/state-schema
              :sys-state-source {:temp-branch/db-id nil}})

(u/configure-capsule-logging :info)


(deftest test-create-branch-nil-src
  (au/test-async
   10000
   (ca/go
     (let [branch "test123"
           get-server-url (make-get-server-url "admin-client")
           get-admin-creds (constantly {:subject-id "admin-client"
                                        :subject-secret ""})
           ac (ac/admin-client get-server-url get-admin-creds)
           _ (is (= true (au/<? (ac/<create-branch ac branch nil))))
           vc (vivo/vivo-client (assoc vc-opts :sys-state-source
                                       {:branch/name branch}))]
       (try
         (let [app-name "my app name"
               set-ret (au/<? (vivo/<set-state! vc [:sys :app-name] app-name))
               _ (is (= true set-ret))
               sub-map '{app-name [:sys :app-name]}
               sub-name "test-sub"
               state-ret (vivo/subscribe-to-state! vc sub-name sub-map
                                                   (constantly nil))]
           (is (= app-name ('app-name state-ret)))
           (is (= nil (vivo/unsubscribe-from-state! vc sub-name))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e))
           (log/error (str "Exception in test-large-data-storage:\n"
                           (u/ex-msg-and-stacktrace e))))
         (finally
           (is (= true (au/<? (ac/<delete-branch ac branch))))
           (vivo/shutdown! vc)
           (ac/shutdown! ac)))))))

(deftest test-create-temp-branch-nil-src
  (au/test-async
   10000
   (ca/go
     (let [vc (vivo/vivo-client (assoc vc-opts :sys-state-source
                                       {:temp-branch/db-id nil}))]
       (try
         (let [app-name "my app name"
               set-ret (au/<? (vivo/<set-state! vc [:sys :app-name] app-name))
               _ (is (= true set-ret))
               sub-map '{app-name [:sys :app-name]}
               sub-name "test-sub"
               state-ret (vivo/subscribe-to-state! vc sub-name sub-map
                                                   (constantly nil))]
           (is (= app-name ('app-name state-ret)))
           (is (= nil (vivo/unsubscribe-from-state! vc sub-name))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e))
           (log/error (str "Exception in test-large-data-storage:\n"
                           (u/ex-msg-and-stacktrace e))))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-create-branch-bad-src-db-id
  (au/test-async
   10000
   (ca/go
     (let [branch "test123"
           db-id "a db-id that doesn't exist"
           get-server-url (make-get-server-url "admin-client")
           get-admin-creds (constantly {:subject-id "admin-client"
                                        :subject-secret ""})
           ac (ac/admin-client get-server-url get-admin-creds)]
       (is (thrown-with-msg?
            #?(:clj ExceptionInfo :cljs js/Error)
            #"Source db-id .* does not exist"
            (au/<? (ac/<create-branch ac branch db-id))))
       (ac/shutdown! ac)))))

(deftest test-create-branch-db-id-src
  (au/test-async
   10000
   (ca/go
     (let [branch1 "xyz1"
           branch2 "xyz2"
           app-name1 "my app name1"
           app-name2 "my app name2"
           get-server-url (make-get-server-url "admin-client")
           get-admin-creds (constantly {:subject-id "admin-client"
                                        :subject-secret ""})
           ac (ac/admin-client get-server-url get-admin-creds)
           _ (is (= true (au/<? (ac/<create-branch ac branch1 nil))))
           vc1 (vivo/vivo-client (assoc vc-opts :sys-state-source
                                        {:branch/name branch1}))
           set-ret1 (au/<? (vivo/<set-state! vc1 [:sys :app-name] app-name1))
           _ (is (= true set-ret1))
           sub-map '{app-name [:sys :app-name]}
           sub-name "test-sub"
           state-ret1 (vivo/subscribe-to-state! vc1 sub-name sub-map
                                                (constantly nil))
           _ (is (= app-name1 ('app-name state-ret1)))
           db-id1 (au/<? (ac/<get-db-id-for-branch ac branch1))
           _ (is (= true (au/<? (ac/<create-branch ac branch2 db-id1))))
           vc2 (vivo/vivo-client (assoc vc-opts :sys-state-source
                                        {:branch/name branch2}))
           _ (au/<? (u/<wait-for-conn-init vc2))
           state-ret2 (vivo/subscribe-to-state! vc2 sub-name sub-map
                                                (constantly nil))
           _ (is (= app-name1 ('app-name state-ret2)))
           set-ret2 (au/<? (vivo/<set-state! vc2 [:sys :app-name] app-name2))
           _ (is (= true set-ret1))
           state-ret3 (vivo/subscribe-to-state! vc2 sub-name sub-map
                                                (constantly nil))]
       (is (= app-name2 ('app-name state-ret3)))
       (is (= true (au/<? (ac/<delete-branch ac branch2))))
       (is (= true (au/<? (ac/<delete-branch ac branch1))))
       (vivo/shutdown! vc1)
       (vivo/shutdown! vc2)
       (ac/shutdown! ac)))))

(deftest test-create-temp-branch-db-id-src
  (au/test-async
   10000
   (ca/go
     (let [branch1 "xyz1"
           branch2 "xyz2"
           app-name1 "my app name1"
           app-name2 "my app name2"
           get-server-url (make-get-server-url "admin-client")
           get-admin-creds (constantly {:subject-id "admin-client"
                                        :subject-secret ""})
           ac (ac/admin-client get-server-url get-admin-creds)
           _ (is (= true (au/<? (ac/<create-branch ac branch1 nil))))
           vc1 (vivo/vivo-client (assoc vc-opts :sys-state-source
                                        {:branch/name branch1}))
           set-ret1 (au/<? (vivo/<set-state! vc1 [:sys :app-name] app-name1))
           _ (is (= true set-ret1))
           sub-map '{app-name [:sys :app-name]}
           sub-name "test-sub"
           state-ret1 (vivo/subscribe-to-state! vc1 sub-name sub-map
                                                (constantly nil))
           _ (is (= app-name1 ('app-name state-ret1)))
           db-id1 (au/<? (ac/<get-db-id-for-branch ac branch1))
           vc2 (vivo/vivo-client (assoc vc-opts :sys-state-source
                                        {:temp-branch/db-id db-id1}))
           _ (au/<? (u/<wait-for-conn-init vc2))
           state-ret2 (vivo/subscribe-to-state! vc2 sub-name sub-map
                                                (constantly nil))
           _ (is (= app-name1 ('app-name state-ret2)))
           set-ret2 (au/<? (vivo/<set-state! vc2 [:sys :app-name] app-name2))
           _ (is (= true set-ret1))
           state-ret3 (vivo/subscribe-to-state! vc2 sub-name sub-map
                                                (constantly nil))]
       (is (= app-name2 ('app-name state-ret3)))
       (is (= true (au/<? (ac/<delete-branch ac branch2))))
       (is (= true (au/<? (ac/<delete-branch ac branch1))))
       (vivo/shutdown! vc1)
       (vivo/shutdown! vc2)
       (ac/shutdown! ac)))))
