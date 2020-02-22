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
   [deercreeklabs.capsule.logging :as logging]
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
               index-ch (ca/chan 1)
               expected-all-msgs [{:text "A msg"
                                   :user {:name "Bo Johnson"
                                          :nickname "Bo"}}
                                  {:text "This is great"
                                   :user {:name "Bo Johnson"
                                          :nickname "Bo"}}]]
           (vivo/subscribe! vc '{msgs [:sys :msgs]
                                 users [:sys :users]}
                            nil
                            (fn [{:syms [msgs users] :as arg}]
                              (if-not (seq msgs)
                                (ca/put! all-msgs-ch :no-msgs)
                                (let [msgs* (join-msgs-and-users msgs users)]
                                  (ca/put! all-msgs-ch msgs*))))
                            "test1")
           (vivo/subscribe! vc '{app-name [:sys :app-name]} nil
                            (fn [df]
                              (if-let [app-name (df 'app-name)]
                                (ca/put! app-name-ch app-name)
                                (ca/put! app-name-ch :no-name)))
                            "test2")
           (vivo/subscribe! vc '{last-msg [:sys :msgs -1]} nil
                            (fn [df]
                              (if-let [last-msg (df 'last-msg)]
                                (ca/put! last-msg-ch last-msg)
                                (ca/put! last-msg-ch :no-last)))
                            "test3")
           (vivo/subscribe! vc '{uid->msgs [:sys :user-id-to-msgs]} nil
                            (fn [{:syms [uid->msgs] :as df}]
                              (if (seq uid->msgs)
                                (ca/put! index-ch uid->msgs)
                                (ca/put! index-ch :no-u->m)))
                            "test4")
           (is (= :no-msgs (au/<? all-msgs-ch))) ; initial subscription result
           (is (= :no-name (au/<? app-name-ch))) ; initial subscription result
           (is (= :no-last (au/<? last-msg-ch))) ; initial subscription result
           (is (= :no-u->m (au/<? index-ch))) ; initial subscription result
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
           (is (= app-name (au/<? app-name-ch)))
           (is (= msg2 (au/<? last-msg-ch)))
           (is (= {"1" [{:text "This is great" :user-id "1"}
                        {:text "A msg" :user-id "1"}]}
                  (au/<? index-ch)))
           (is (= true (au/<? (vivo/<update-state!
                               vc [{:path [:sys :msgs -1]
                                    :op :remove}]))))
           (is (= msg (au/<? last-msg-ch)))
           (is (= 1 (count (au/<? all-msgs-ch))))
           (is (= {"1" [{:text "A msg" :user-id "1"}]}
                  (au/<? index-ch))))
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
         (let [ch (ca/chan 1)
               core-user-ids ["123" "789"]
               resolution-map {'core-user-ids core-user-ids}
               users {"123" {:name "Alice" :nickname "A"}
                      "456" {:name "Bob" :nickname "Bobby"}
                      "789" {:name "Candace" :nickname "Candy"}}
               sub-map '{core-user-names [:sys :users core-user-ids :name]}
               update-fn #(ca/put! ch ('core-user-names %))
               expected ["Alice" "Candace"]]
           (au/<? (vivo/<update-state! vc [{:path [:sys :users]
                                            :op :set
                                            :arg users}]))
           (vivo/subscribe! vc sub-map nil update-fn "test" resolution-map)
           (is (= expected (au/<? ch))))
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
         (let [ch (ca/chan 1)
               users {"123" {:name "Alice" :nickname "A"}
                      "456" {:name "Bob" :nickname "Bobby"}
                      "789" {:name "Candace" :nickname "Candy"}}
               sub-map '{core-user-ids [:sys :core-user-ids]
                         core-user-names [:sys :users core-user-ids :name]}
               update-fn #(ca/put! ch %)
               expected '{core-user-ids []
                          core-user-names nil}]
           (au/<? (vivo/<update-state! vc [{:path [:sys :core-user-ids]
                                            :op :set
                                            :arg []}
                                           {:path [:sys :users]
                                            :op :set
                                            :arg users}]))
           (vivo/subscribe! vc sub-map nil update-fn "test")
           (is (= expected (au/<? ch))))
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
         (let [ch (ca/chan 1)
               users {"123" {:name "Alice" :nickname "A"}
                      "456" {:name "Bob" :nickname "Bobby"}
                      "789" {:name "Candace" :nickname "Candy"}}
               sub-map '{core-user-names [:sys :users ["123" "789"] :name]}
               update-fn #(ca/put! ch %)
               expected {'core-user-names ["Alice" "Candace"]}]
           (au/<? (vivo/<set-state! vc [:sys :users] users))
           (vivo/subscribe! vc sub-map nil update-fn "test")
           (is (= expected (au/<? ch))))
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
         (let [ch (ca/chan 1)
               users {"123" {:name "Alice" :nickname "A"}
                      "456" {:name "Bob" :nickname "Bobby"}
                      "789" {:name "Candace" :nickname "Candy"}}
               sub-map '{user-name [:sys :users "999" :name]}
               update-fn #(ca/put! ch %)
               expected {'user-name nil}]
           (au/<? (vivo/<set-state! vc [:sys :users] users))
           (vivo/subscribe! vc sub-map nil update-fn "test")
           (is (= expected (au/<? ch))))
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
         (let [ch (ca/chan 1)
               users {"123" {:name "Alice" :nickname "A"}
                      "456" {:name "Bob" :nickname "Bobby"}
                      "789" {:name "Candace" :nickname "Candy"}}
               sub-map '{core-users [:sys :users ["999"]]}
               update-fn #(ca/put! ch %)
               expected '{core-users [nil]}]
           (au/<? (vivo/<set-state! vc [:sys :users] users))
           (vivo/subscribe! vc sub-map nil update-fn "test")
           (is (= expected (au/<? ch))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-authentication
  (au/test-async
   10000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [sid (au/<? (vivo/<add-subject! vc tu/test-identifier
                                              tu/test-secret
                                              tu/test-subject-id))
               _ (is (string? sid))
               state-ch (ca/chan)
               sub-map '{subject-id :vivo/subject-id}
               unsub! (vivo/subscribe! vc sub-map nil #(ca/put! state-ch %)
                                       "test")
               _ (is (= {'subject-id nil} (au/<? state-ch)))
               login-ret (vivo/log-in!
                           vc (str/upper-case tu/test-identifier)
                           tu/test-secret)]
           (when-not login-ret
             (throw (ex-info "Login failed. This is unexpected."
                             (u/sym-map login-ret))))
           (is (= tu/test-subject-id ('subject-id (au/<? state-ch))))
           (au/<? (vivo/<log-out! vc))
           (is (= {'subject-id nil} (au/<? state-ch)))
           (unsub!))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected (u/ex-msg e))))
         (finally
           (vivo/shutdown! vc)))))))

;; TODO: Figure out why these tests cause errors
#_
(deftest test-secret-too-long
  (au/test-async
   10000
   (au/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [secret-len (inc u/max-secret-len)
               long-secret (apply str (repeat secret-len "*"))]
           (is (thrown-with-msg?
                #?(:clj ExceptionInfo :cljs js/Error)
                #"Secrets must not be longer than"
                (au/<? (vivo/<add-subject! vc tu/test-identifier
                                           long-secret
                                           tu/test-subject-id))))
           (is (thrown-with-msg?
                #?(:clj ExceptionInfo :cljs js/Error)
                #"Secrets must not be longer than"
                (au/<? (au/<? (vivo/<log-in!
                               vc tu/test-identifier long-secret)))))
           (is (thrown-with-msg?
                #?(:clj ExceptionInfo :cljs js/Error)
                #"Secrets must not be longer than"
                (au/<? (au/<? (vivo/<change-secret!
                               vc long-secret))))))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected (u/ex-msg e))))
         (finally
           (vivo/shutdown! vc)))))))

(deftest test-authorization
  (au/test-async
   10000
   (ca/go
     (let [vc (vivo/vivo-client vc-opts)]
       (try
         (let [sid (au/<? (vivo/<add-subject! vc tu/test-identifier
                                              tu/test-secret
                                              tu/test-subject-id))
               _ (is (string? sid))
               app-name "test-app"
               ret (au/<? (vivo/<set-state! vc [:sys :app-name] app-name))
               _ (is (= true ret))
               state-ch (ca/chan)
               sub-map '{app-name [:sys :app-name]
                         secret [:sys :secret]}
               unsub! (vivo/subscribe! vc sub-map nil #(ca/put! state-ch %)
                                       "test")
               expected-state {'app-name app-name
                               'secret :vivo/unauthorized}
               _ (is (= expected-state (au/<? state-ch)))
               _ (is (= :vivo/unauthorized (au/<? (vivo/<set-state!
                                                   vc [:sys :secret] "Foo"))))
               _ (is (thrown-with-msg?
                      #?(:clj ExceptionInfo :cljs js/Error)
                      #"RPC `:authed/inc` is unauthorized"
                      (au/<? (vivo/<rpc vc :authed/inc 1 10000))))
               login-ret (au/<? (vivo/<log-in! vc tu/test-identifier
                                               tu/test-secret))
               {:keys [subject-id token]} login-ret]
           (is (= tu/test-subject-id subject-id))
           (is (string? token))
           (is (= 2 (au/<? (vivo/<rpc vc :authed/inc 1 10000))))
           (unsub!))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e)))
         (finally
           (vivo/shutdown! vc)))))))

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
           (println "Exception in test-rpc:\n" (u/ex-msg-and-stacktrace e)))
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
               state-ch (ca/chan)
               sub-map '{app-name [:sys :app-name]}
               unsub! (vivo/subscribe! vc sub-map nil #(ca/put! state-ch %)
                                       "test")
               state-ret (au/<? state-ch)]
           (is (= huge-name ('app-name state-ret)))
           (unsub!))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (= :unexpected e))
           (println "Exception in test-large-data-storage:\n"
                    (u/ex-msg-and-stacktrace e)))
         (finally
           (is (= true (au/<? (ac/<delete-branch ac branch))))
           (vivo/shutdown! vc)
           (ac/shutdown! ac)))))))
