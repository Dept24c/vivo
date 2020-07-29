(ns com.dept24c.integration.integration-test
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [com.dept24c.vivo :as vivo]
   [com.dept24c.vivo.admin-client :as ac]
   [com.dept24c.vivo.state-schema :as ss]
   [com.dept24c.vivo.test-user :as tu]
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

(def user-bo {:name "Bo Johnson"
              :nickname "Bo"})
(def user-bo-id "1")

(u/configure-capsule-logging :info)

(defn join-msgs-and-users [msgs users]
  (reduce (fn [acc {:keys [user-id text] :as msg}]
            (if-not msg
              acc
              (conj acc {:user (users user-id)
                         :text text})))
          [] msgs))

(deftest test-subscriptions
  (au/test-async
   5000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [app-name "test-app"
               msg {:user-id user-bo-id
                    :text "A msg"}
               msg2 (assoc msg :text "This is great")
               last-msg-ch (ca/chan 1)
               all-msgs-ch (ca/chan 1)
               app-name-ch (ca/chan 1)
               expected-all-msgs [{:text "A msg"
                                   :user {:name "Bo Johnson"
                                          :nickname "Bo"}}
                                  {:text "This is great"
                                   :user {:name "Bo Johnson"
                                          :nickname "Bo"}}]
               all-msgs-ufn (fn [{:syms [msgs users] :as arg}]
                              (if-not (seq msgs)
                                (ca/put! all-msgs-ch :no-msgs)
                                (let [msgs* (join-msgs-and-users msgs users)]
                                  (ca/put! all-msgs-ch msgs*))))]
           (is (= {'msgs nil
                   'users nil}
                  (vivo/subscribe-to-state! vc "test1"
                                   '{msgs [:sys :msgs]
                                     users [:sys :users]}
                                   all-msgs-ufn)))
           (is (= {'app-name nil}
                  (vivo/subscribe-to-state! vc "test2"
                                   '{app-name [:sys :app-name]}
                                   #(ca/put! app-name-ch %))))
           (is (= {'last-msg nil}
                  (vivo/subscribe-to-state! vc "test3"
                                   '{last-msg [:sys :msgs -1]}
                                   #(ca/put! last-msg-ch %))))
           (is (= true (au/<? (vivo/<update-state!
                               vc [{:path [:sys]
                                    :op :set
                                    :arg {:app-name app-name
                                          :msgs []
                                          :users {user-bo-id user-bo}}}
                                   {:path [:sys :msgs -1]
                                    :op :insert-after
                                    :arg msg}
                                   {:path [:sys :msgs -1]
                                    :op :insert-after
                                    :arg msg2}]))))
           (is (= expected-all-msgs (au/<? all-msgs-ch)))
           (is (= {'app-name app-name} (au/<? app-name-ch)))
           (is (= {'last-msg msg2} (au/<? last-msg-ch)))
           (is (= true (au/<? (vivo/<update-state!
                               vc [{:path [:sys :msgs -1]
                                    :op :remove}]))))
           (is (= {'last-msg msg} (au/<? last-msg-ch)))
           (is (= 1 (count (au/<? all-msgs-ch)))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-subscriptions-evolution
  (au/test-async
   5000
   (ca/go
     (let [vc-opts2 {:get-server-url (make-get-server-url "vivo-client")
                     :rpcs ss/rpcs2
                     :sys-state-schema ss/state-schema2
                     :sys-state-source {:temp-branch/db-id nil}}
           vc (vivo/vivo-client vc-opts2)]
       (try
         (let [program-name "test-app"
               program-name-ch (ca/chan 1)
               users-ch (ca/chan 1)
               users {user-bo-id user-bo}]
           (is (= {'program-name nil}
                  (vivo/subscribe-to-state!
                   vc "test2a"
                   '{program-name [:sys :program-name]}
                   #(ca/put! program-name-ch %))))
           (is (= {'users nil}
                  (vivo/subscribe-to-state! vc "test1a"
                                            '{users [:sys :users]}
                                            #(ca/put! users-ch %))))
           (is (= true (au/<? (vivo/<update-state!
                               vc [{:path [:sys]
                                    :op :set
                                    :arg {:program-name program-name
                                          :users users}}]))))
           (is (= {'users users} (au/<? users-ch))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-sequence-join
  (au/test-async
   10000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [core-user-ids ["123" "789"]
               resolution-map {'core-user-ids core-user-ids}
               users {"123" {:name "Alice" :nickname "A"}
                      "456" {:name "Bob" :nickname "Bobby"}
                      "789" {:name "Candace" :nickname "Candy"}}
               sub-map '{core-user-names [:sys :users core-user-ids :name]}
               expected '{core-user-names ["Alice" "Candace"]}]
           (au/<? (vivo/<update-state! vc [{:path [:sys :users]
                                            :op :set
                                            :arg users}]))
           (is (= expected
                  (vivo/subscribe-to-state!
                   vc "test" sub-map (constantly nil)
                   (u/sym-map resolution-map)))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-empty-sequence-join
  (au/test-async
   3000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [users {"123" {:name "Alice" :nickname "A"}
                      "456" {:name "Bob" :nickname "Bobby"}
                      "789" {:name "Candace" :nickname "Candy"}}
               sub-map '{core-user-ids [:sys :core-user-ids]
                         core-user-names [:sys :users core-user-ids :name]}
               expected '{core-user-ids []
                          core-user-names nil}]
           (is (= true
                  (au/<? (vivo/<update-state! vc [{:path [:sys :core-user-ids]
                                                   :op :set
                                                   :arg []}
                                                  {:path [:sys :users]
                                                   :op :set
                                                   :arg users}]))))
           (is (= expected (vivo/subscribe-to-state! vc "test"
                                                     sub-map nil))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-explicit-seq-in-path
  (au/test-async
   3000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [users {"123" {:name "Alice" :nickname "A"}
                      "456" {:name "Bob" :nickname "Bobby"}
                      "789" {:name "Candace" :nickname "Candy"}}
               sub-map '{core-user-names [:sys :users ["123" "789"] :name]}
               expected {'core-user-names ["Alice" "Candace"]}]
           (is (= true (au/<? (vivo/<set-state! vc [:sys :users] users))))
           (is (= expected (vivo/subscribe-to-state! vc "test"
                                                     sub-map nil))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-nil-return
  (au/test-async
   3000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [users {"123" {:name "Alice" :nickname "A"}
                      "456" {:name "Bob" :nickname "Bobby"}
                      "789" {:name "Candace" :nickname "Candy"}}
               sub-map '{user-name [:sys :users "999" :name]}
               expected {'user-name nil}]
           (is (= true (au/<? (vivo/<set-state! vc [:sys :users] users))))
           (is (= expected (vivo/subscribe-to-state! vc "test"
                                                     sub-map nil))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-seq-w-nil-return
  (au/test-async
   3000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [users {"123" {:name "Alice" :nickname "A"}
                      "456" {:name "Bob" :nickname "Bobby"}
                      "789" {:name "Candace" :nickname "Candy"}}
               sub-map '{core-users [:sys :users ["999"]]}
               expected '{core-users [nil]}]
           (is (= true (au/<? (vivo/<set-state! vc [:sys :users] users))))
           (is (= expected (vivo/subscribe-to-state! vc "test"
                                                     sub-map nil))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-kw-operators
  (au/test-async
   10000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [users {"123" {:name "Alice" :nickname "A" :fav-nums [1 2]}
                      "456" {:name "Bob" :nickname "Bobby" :fav-nums [10 20]}
                      "789" {:name "Candace" :nickname "Candy" :fav-nums [3 4]}}
               msgs [{:text "hi" :user-id "123"}
                     {:text "there" :user-id "123"}]
               sub-map '{num-users [:sys :users :vivo/count]
                         user-ids [:sys :users :vivo/keys]
                         user-names-1 [:sys :users user-ids :name]
                         user-names-2 [:sys :users :vivo/* :name]
                         fav-nums [:sys :users :vivo/* :fav-nums :vivo/concat]
                         num-msgs [:sys :msgs :vivo/count]
                         msgs [:sys :msgs]
                         msg-indices [:sys :msgs :vivo/keys]}
               expected {'num-users 3
                         'user-ids #{"123" "456" "789"}
                         'user-names-1 #{"Alice" "Bob" "Candace"}
                         'user-names-2 #{"Alice" "Bob" "Candace"}
                         'fav-nums #{1 2 10 20 3 4}
                         'num-msgs 2
                         'msg-indices [0 1]
                         'msgs msgs}
               update-ret (au/<? (vivo/<update-state!
                                  vc [{:path [:sys]
                                       :op :set
                                       :arg (u/sym-map msgs users)}]))
               _ (is (= true update-ret))
               sub-ret (vivo/subscribe-to-state! vc "test" sub-map nil)]
           (is (= expected
                  (-> sub-ret
                      (update 'user-ids set)
                      (update 'user-names-1 set)
                      (update 'user-names-2 set)
                      (update 'fav-nums set)))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-secret-too-long
  (au/test-async
   10000
   (au/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [secret-len (inc u/max-secret-len)
               long-secret (apply str (repeat secret-len "*"))
               short-secret "short"
               ;; Bizarrely, these `thrown-with-msg?` calls must be inside
               ;; the `let` or they cause extraneous errors
               _ (is (thrown-with-msg?
                      #?(:clj ExceptionInfo :cljs js/Error)
                      #"Secrets must not be longer than"
                      (au/<? (vivo/<add-subject! vc nil long-secret nil))))
               _ (is (thrown-with-msg?
                      #?(:clj ExceptionInfo :cljs js/Error)
                      #"Secrets must not be longer than"
                      (au/<? (vivo/<log-in!
                              vc tu/test-identifier long-secret))))
               _ (is (thrown-with-msg?
                      #?(:clj ExceptionInfo :cljs js/Error)
                      #"Secrets must not be longer than"
                      (au/<? (vivo/<change-secret!
                              vc short-secret long-secret))))
               _ (is (thrown-with-msg?
                      #?(:clj ExceptionInfo :cljs js/Error)
                      #"Secrets must not be longer than"
                      (au/<? (vivo/<change-secret!
                              vc long-secret short-secret))))])
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected (u/ex-msg e))))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-change-secret
  (au/test-async
   10000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [sid (au/<? (vivo/<add-subject! vc tu/test-identifier
                                              tu/test-secret
                                              tu/test-subject-id))
               _ (is (= tu/test-subject-id sid))
               login-1-ret (au/<? (vivo/<log-in! vc tu/test-identifier
                                                 tu/test-secret))
               _ (is (= tu/test-subject-id (:subject-id login-1-ret)))
               new-secret "new-secret!!"
               change-ret (au/<? (vivo/<change-secret! vc tu/test-secret
                                                       new-secret))
               _ (is (= true change-ret))
               logout-ret (au/<? (vivo/<log-out! vc))
               _ (is (= true logout-ret))
               token-login-ret (au/<? (vivo/<log-in-w-token!
                                       vc (:token login-1-ret)))
               _ (is (= false token-login-ret))
               login-2-ret (au/<? (vivo/<log-in! vc tu/test-identifier
                                                 tu/test-secret))
               _ (is (= false login-2-ret))
               login-3-ret (au/<? (vivo/<log-in! vc tu/test-identifier
                                                 new-secret))]
           (is (string? (:token login-3-ret)))
           (is (= tu/test-subject-id (:subject-id login-3-ret))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-add-remove-identifier
  (au/test-async
   10000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [new-identifier "me@emailhaven.com"
               sid (au/<? (vivo/<add-subject! vc tu/test-identifier
                                              tu/test-secret
                                              tu/test-subject-id))
               _ (is (= tu/test-subject-id sid))
               add-1-ret (au/<? (vivo/<add-subject-identifier!
                                 vc new-identifier))
               _ (is (= false add-1-ret)) ;; Not logged in yet
               remove-1-ret (au/<? (vivo/<remove-subject-identifier!
                                    vc new-identifier))
               _ (is (= false remove-1-ret)) ;; Not logged in yet
               login-1-ret (au/<? (vivo/<log-in! vc tu/test-identifier
                                                 tu/test-secret))
               _ (is (= tu/test-subject-id (:subject-id login-1-ret)))
               add-2-ret (au/<? (vivo/<add-subject-identifier!
                                 vc new-identifier))
               _ (is (= true add-2-ret))
               remove-2-ret (au/<? (vivo/<remove-subject-identifier!
                                    vc tu/test-identifier))
               _ (is (= true remove-2-ret))
               logout-ret (au/<? (vivo/<log-out! vc))
               _ (is (= true logout-ret))
               ;; Using old identifier fails
               login-2-ret (au/<? (vivo/<log-in! vc tu/test-identifier
                                                 tu/test-secret))
               _ (is (= false login-2-ret))
               ;; Using new identifier works
               login-3-ret (au/<? (vivo/<log-in! vc new-identifier
                                                 tu/test-secret))]
           (is (string? (:token login-3-ret)))
           (is (= tu/test-subject-id (:subject-id login-3-ret))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-authentication-and-authorization
  (au/test-async
   10000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)
           vc2 (vivo/vivo-client vc-opts)]
       (try
         (let [login-ret1 (au/<? (vivo/<log-in! vc tu/test-identifier
                                                tu/test-secret))
               _ (is (= false login-ret1))
               sid (au/<? (vivo/<add-subject! vc tu/test-identifier
                                              tu/test-secret
                                              tu/test-subject-id))
               _ (is (string? sid))
               app-name "test-app"
               ret (au/<? (vivo/<set-state! vc [:sys :app-name] app-name))
               _ (is (= true ret))
               sub-map '{app-name [:sys :app-name]
                         launch-codes [:sys :secret :launch-codes]}
               state (vivo/subscribe-to-state! vc "test" sub-map
                                               (constantly nil))
               expected-state {'app-name app-name
                               'launch-codes nil}
               _ (is (= expected-state state))
               _ (is (= :vivo/unauthorized
                        (au/<? (vivo/<set-state!
                                vc [:sys :secret :launch-codes] ["Foo"]))))
               _ (is (thrown-with-msg?
                      #?(:clj ExceptionInfo :cljs js/Error)
                      #"RPC `:authed/inc` is unauthorized"
                      (au/<? (vivo/<rpc vc :authed/inc 1 10000))))
               login-ret2 (au/<? (vivo/<log-in! vc tu/test-identifier
                                                tu/test-secret))
               {:keys [subject-id token]} login-ret2
               _ (is (= tu/test-subject-id subject-id))
               _ (is (string? token))
               token-login-2-ret (au/<? (vivo/<log-in-w-token!
                                         vc token))
               _ (is (= (u/sym-map subject-id token) token-login-2-ret))
               _ (is (= 2 (au/<? (vivo/<rpc vc :authed/inc 1 10000))))
               _ (vivo/unsubscribe-from-state! vc "test")
               logout-ret (au/<? (vivo/<log-out! vc))
               _ (is (= true logout-ret))
               token-login-ret (au/<? (vivo/<log-in-w-token!
                                       vc token))
               _ (is (= false token-login-ret))])
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)
           (vivo/shutdown! vc2)))))))

(l/def-record-schema complex-num-schema
  [:real-part l/double-schema]
  [:imaginary-part l/double-schema])

(deftest test-schema<->fp
  (au/test-async
   10000
   (ca/go
     (let [vc1 (vivo/vivo-client vc-opts)
           ;; Make a separate vc to get around local cache and test
           ;; server-side durability
           vc2 (vivo/vivo-client vc-opts)]
       (try
         (let [fp (au/<? (vivo/<schema->fp vc1 complex-num-schema))
               expected-fp (l/fingerprint64 complex-num-schema)
               _ (is (= expected-fp fp))
               rt-schema (au/<? (vivo/<fp->schema vc2 fp))
               _ (is (= (l/pcf complex-num-schema)
                        (l/pcf rt-schema)))
               data {:real-part 10.6
                     :imaginary-part 42.7}
               encoded (l/serialize rt-schema data)
               decoded (l/deserialize complex-num-schema rt-schema encoded)]
           (is (= data decoded)))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc1)
           (vivo/shutdown! vc2)))))))

(deftest test-rpc
  (au/test-async
   10000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [ret (au/<? (vivo/<rpc vc :inc 1 5000))]
           (is (= 2 ret)))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e))
           (log/error (str "Exception in test-rpc:\n"
                           (u/ex-msg-and-stacktrace e))))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-ddb-large-data-storage
  (au/test-async
   10000
   (ca/go
     (let [branch "test"
           get-server-url (make-get-server-url "admin-client")
           get-admin-creds (constantly {:subject-id "admin-client"
                                        :subject-secret ""})
           ac (ac/admin-client get-server-url get-admin-creds)
           _ (is (= true (au/<? (ac/<create-branch ac branch nil))))
           vc (vivo/vivo-client (assoc vc-opts :sys-state-source
                                       {:branch/name branch}))]
       (try
         (let [huge-name (apply str (take 500000 (repeat "x")))
               set-ret (au/<? (vivo/<set-state! vc [:sys :app-name] huge-name))
               _ (is (= true set-ret))
               sub-map '{app-name [:sys :app-name]}
               state-ret (vivo/subscribe-to-state! vc "test" sub-map
                                                   (constantly nil))]
           (is (= huge-name ('app-name state-ret)))
           (vivo/unsubscribe-from-state! vc "test"))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e))
           (log/error (str "Exception in test-large-data-storage:\n"
                           (u/ex-msg-and-stacktrace e))))
         (finally
           (is (= true (au/<? (ac/<delete-branch ac branch))))
           (vivo/shutdown! vc)
           (ac/shutdown! ac)))))))

;; TODO: Enable this when :sys-msgs are implemented
#_
(deftest test-sys-pub-sub
  (au/test-async
   10000
   (ca/go
     (let [vc1 (vivo/vivo-client vc-opts)
           vc2 (vivo/vivo-client vc-opts)]
       (try
         (let [ch (ca/chan)
               cb #(ca/put! ch %)
               sub-name "my-sub"
               msg-name "a-msg"
               msg-val "the-val"
               sub-map {'foo [:sys-msgs msg-name]}
               _ (is (= true (au/<? (vivo/<wait-for-conn-init vc1))))
               _ (is (= true (au/<? (vivo/<wait-for-conn-init vc2))))
               ret1 (vivo/subscribe-to-state! vc1 sub-name sub-map cb)
               _ (is (= {'foo nil} ret1))
               ret2 (vivo/publish! vc2 :sys-msgs msg-name msg-val)
               _ (is (= nil ret2))
               expected {'foo msg-val}]
           (is (= expected (au/<? ch)))
           (is (= nil (vivo/unsubscribe-from-state! vc1 sub-name))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc1)
           (vivo/shutdown! vc2)))))))
