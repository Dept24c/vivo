(ns com.dept24c.unit.vivo-test
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [clojure.test :refer [are deftest is]]
   [com.dept24c.vivo :as vivo]
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.macro-impl :as macro-impl]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-parse-def-component-args-no-docstring
  (let [arglist '[vc a b]
        sub-map '{x [:local :c]}
        args [arglist sub-map]
        expected {:docstring nil
                  :sub-map sub-map
                  :arglist arglist
                  :body nil}]
    (is (= expected (macro-impl/parse-def-component-args 'foo args)))))

(deftest test-parse-def-component-args-with-docstring
  (let [docstring "This is my component"
        arglist '[vc a b]
        sub-map '{x [:local :c]}
        args [docstring arglist sub-map]
        expected {:docstring docstring
                  :sub-map sub-map
                  :arglist arglist
                  :body nil}]
    (is (= expected (macro-impl/parse-def-component-args 'foo args)))))

(deftest test-parse-def-component-args-bad-arg
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"The argument list must be a vector"
       (macro-impl/parse-def-component-args 'foo nil))))

(deftest test-check-arglist-no-vc
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"First argument must be `vc`"
       (macro-impl/check-arglist 'foo '[]))))

(deftest test-check-arglist-no-vc-2
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"First argument must be `vc`"
       (macro-impl/check-arglist 'foo '[x]))))

(deftest test-check-arglist
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"The argument list must be a vector"
       (macro-impl/check-arglist 'foo '()))))

(deftest test-repeat-symbol
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Illegal repeated symbol"
       (macro-impl/parse-def-component-args
        'foo ['[vc a] '{a [:local :a]}]))))

(deftest test-check-constructor-args
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Wrong number of arguments .+ Should have gotten 3 .+ got 1"
       (macro-impl/check-constructor-args "foo" [1] 3))))

(deftest test-edn<->str
  (are [edn] (= edn (u/str->edn (u/edn->str edn)))
    "string"
    {:a 1 :b 2}
    [7 :v :x "str"]
    {"s" 1 :s 2}))

(deftest test-empty-sub-map
  (let [vc (vivo/vivo-client)
        bad-sub-map {}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"The sub-map parameter must contain at least one entry"
         (vivo/subscribe! vc bad-sub-map nil (constantly true) "test")))))

(deftest test-nil-sub-map
  (let [vc (vivo/vivo-client)
        bad-sub-map nil]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"The sub-map parameter must be a map"
         (vivo/subscribe! vc bad-sub-map nil (constantly true) "test")))))

(deftest test-non-sym-key-in-sub-map
  (let [vc (vivo/vivo-client)
        bad-sub-map {:not-a-symbol [:local :user-id]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Keys must be symbols"
         (vivo/subscribe! vc bad-sub-map nil (constantly true) "test")))))

(deftest test-bad-path-in-sub-map
  (let [vc (vivo/vivo-client)
        bad-sub-map '{user-id [:local nil]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Only integers, keywords, symbols, and strings are valid path keys"
         (vivo/subscribe! vc bad-sub-map nil (constantly true) "test")))))

(deftest test-subscribe-to-subscriber-state-without-subscriber-id
  (let [vc (vivo/vivo-client)
        bad-sub-map '{ss [:subscriber]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Missing subscriber/component id in path"
         (vivo/subscribe! vc bad-sub-map nil (constantly true) "test")))))

(deftest test-subscribe-to-subscriber-state-without-subscriber-id
  (let [vc (vivo/vivo-client)
        bad-sub-map '{cs [:component]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Missing subscriber/component id in path"
         (vivo/subscribe! vc bad-sub-map nil (constantly true) "test")))))

(deftest test-update-subscriber-state-no-sub-id
  (au/test-async
   1000
   (ca/go
     (let [vc (vivo/vivo-client)
           ret (ca/<! (vivo/<set-state! vc [:subscriber] {}))]
       (is (str/includes? (u/ex-msg ret) "Missing subscriber/component id "))))))

(deftest test-subscribe!
  (au/test-async
   1000
   (ca/go
     (let [vc (vivo/vivo-client)
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           name "Alice"
           user-id "123"
           sub-map '{id [:local :user-id]
                     name [:local :users id :name]}
           expected '{id "123"
                      name "Alice"}]
       (au/<? (vivo/<update-state! vc [{:path [:local :users]
                                        :op :set
                                        :arg {user-id {:name name}}}
                                       {:path [:local :user-id]
                                        :op :set
                                        :arg user-id}]))
       (vivo/subscribe! vc sub-map nil update-fn "test")
       (is (= expected (au/<? ch)))))))

(deftest test-subscribe!-subscriber-id
  (au/test-async
   1000
   (ca/go
     (let [vc (vivo/vivo-client)
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           sub-map '{sub-id :vivo/subscriber-id}
           sub-id (vivo/subscribe! vc sub-map nil update-fn "test-sub-id")]
       (is (= sub-id ('sub-id (au/<? ch))))))))

(deftest test-subscribe!-component-id
  (au/test-async
   1000
   (ca/go
     (let [vc (vivo/vivo-client)
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           sub-map '{cid :vivo/component-id}
           cid (vivo/subscribe! vc sub-map nil update-fn "test-cid")]
       (is (= cid ('cid (au/<? ch))))))))

(deftest test-register-subscriber-id!
  (au/test-async
   1000
   (ca/go
     (let [vc (vivo/vivo-client)
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           sub-map '{sub-id :vivo/subscriber-id}
           sub-id (vivo/subscribe! vc sub-map nil update-fn "test-sub-id")
           custom-id "AAA"]
       (vivo/register-subscriber-id! vc custom-id sub-id)
       (is (= sub-id ('sub-id (au/<? ch))))
       (is (= sub-id (vivo/get-subscriber-id vc custom-id)))
       (vivo/unsubscribe! vc sub-id)
       (is (= nil (vivo/get-subscriber-id vc custom-id)))))))

(deftest test-update-subscriber-state
  (au/test-async
   1000
   (ca/go
     (let [vc (vivo/vivo-client)
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           sub-map '{sub-id :vivo/subscriber-id
                     sub-state [:subscriber sub-id]}
           sub-id (vivo/subscribe! vc sub-map nil update-fn "test-sub-id")
           _ (is (nil? ('sub-state (au/<? ch))))
           new-sub-state {:hi :there}
           _ (vivo/set-state! vc [:subscriber sub-id] new-sub-state)]
       (is (= new-sub-state ('sub-state (au/<? ch))))))))

(deftest test-update-component-state
  (au/test-async
   1000
   (ca/go
     (let [vc (vivo/vivo-client)
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           sub-map '{cid :vivo/component-id
                     cstate [:component cid]}
           sub-id (vivo/subscribe! vc sub-map nil update-fn "test-sub-id")
           _ (is (nil? ('cstate (au/<? ch))))
           new-cstate {:hi :there}
           _ (vivo/set-state! vc [:component sub-id] new-cstate)]
       (is (= new-cstate ('cstate (au/<? ch))))))))

(deftest test-subscribe!-single-entry
  (au/test-async
   1000
   (ca/go
     (let [vc (vivo/vivo-client)
           ch (ca/chan 1)
           update-fn #(ca/put! ch (% 'id))
           name "Alice"
           user-id "123"
           sub-map '{id [:local :user-id]}]
       (au/<? (vivo/<update-state! vc [{:path [:local :user-id]
                                        :op :set
                                        :arg user-id}]))
       (vivo/subscribe! vc sub-map nil update-fn "test")
       (is (= user-id (au/<? ch)))))))

(deftest test-simple-insert*
  (let [state [:a :b]
        cases [[[:a :b :new] {:path [-1]
                              :op :insert-after
                              :arg :new}]
               [[:a :new :b] {:path [-1]
                              :op :insert-before
                              :arg :new}]
               [[:new :a :b] {:path [0]
                              :op :insert-before
                              :arg :new}]
               [[:a :new :b] {:path [0]
                              :op :insert-after
                              :arg :new}]
               [[:a :b :new] {:path [1]
                              :op :insert-after
                              :arg :new}]
               [[:a :new :b] {:path [1]
                              :op :insert-before
                              :arg :new}]
               [[:a :b :new] {:path [2]
                              :op :insert-after
                              :arg :new}]
               [[:a :b :new] {:path [10]
                              :op :insert-after
                              :arg :new}]
               [[:new :a :b] {:path [-10]
                              :op :insert-after
                              :arg :new}]
               [[:a :b :new] {:path [10]
                              :op :insert-before
                              :arg :new}]
               [[:new :a :b] {:path [-10]
                              :op :insert-before
                              :arg :new}]]]
    (doseq [case cases]
      (let [[expected {:keys [path op arg] :as cmd}] case
            ret (commands/insert* state path op arg)]
        (is (= case [ret cmd]))))))

(deftest test-deep-insert*
  (let [state {:x [:a :b]}
        cases [[{:x [:a :b :new]} {:path [:x -1]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:a :new :b]} {:path [:x -1]
                                   :op :insert-before
                                   :arg :new}]
               [{:x [:new :a :b]} {:path [:x 0]
                                   :op :insert-before
                                   :arg :new}]
               [{:x [:a :new :b]} {:path [:x 0]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:a :b :new]} {:path [:x 1]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:a :new :b]} {:path [:x 1]
                                   :op :insert-before
                                   :arg :new}]
               [{:x [:a :b :new]} {:path [:x 2]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:a :b :new]} {:path [:x 10]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:new :a :b]} {:path [:x -10]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:a :b :new]} {:path [:x 10]
                                   :op :insert-before
                                   :arg :new}]
               [{:x [:new :a :b]} {:path [:x -10]
                                   :op :insert-before
                                   :arg :new}]
               [{:x [:a :b] :y [:new]} {:path [:y 0]
                                        :op :insert-after
                                        :arg :new}]
               [{:x [:a :b] :y [:new]} {:path [:y 0]
                                        :op :insert-before
                                        :arg :new}]
               [{:x [:a :b] :y [:new]} {:path [:y -1]
                                        :op :insert-before
                                        :arg :new}]
               [{:x [:a :b] :y [:new]} {:path [:y -1]
                                        :op :insert-after
                                        :arg :new}]]]
    (doseq [case cases]
      (let [[expected {:keys [path op arg] :as cmd}] case
            ret (commands/insert* state path op arg)]
        (is (= case [ret cmd]))))))

(deftest test-bad-path-root-in-update-state!
  (let [vc (vivo/vivo-client)]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Paths must begin with "
         (vivo/update-state! vc [{:path [:not-a-valid-root :x]
                                  :op :set
                                  :arg 1}])))))

(deftest test-bad-path-root-in-sub-map
  (let [vc (vivo/vivo-client)
        sub-map '{a [:not-a-valid-root :x]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Paths must begin with "
         (vivo/subscribe! vc sub-map nil (constantly nil) "test")))))

(deftest test-bad-insert*-on-map
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"does not point to a vector"
       (commands/insert* {} [0] :insert-before :new))))

(deftest test-bad-insert*-path
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"the last element of the path must be an integer"
       (commands/insert* [] [] :insert-before :new))))

(deftest test-bad-command-op
  (let [vc (vivo/vivo-client)]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"is not a valid op. Got: `:not-an-op`."
         (vivo/update-state! vc [{:path [:local :x]
                                  :op :not-an-op
                                  :arg 1}])))))

(deftest test-remove
  (let [state {:x [:a :b :c]}
        cases [[{:w 1} {:w 1 :z 2} [:z]]
               [{:w 1 :z {:b 2}} {:w 1 :z {:a 1 :b 2}} [:z :a]]
               [{:x [:b :c]} state [:x 0]]
               [{:x [:a :c]} state [:x 1]]
               [{:x [:a :b]} state [:x 2]]
               [{:x [:a :b]} state [:x -1]]
               [{:x [:a :c]} state [:x -2]]
               [{:x [:a :b :c]} state [:x 10]]
               [{:x [:a :b :c]} state [:x -10]]]]
    (doseq [case cases]
      (let [[expected state* path] case
            ret (commands/eval-cmd state* {:path path :op :remove})]
        (is (= case [ret state* path]))))))

(deftest test-math
  (let [state0 {:a 10}
        state1 {:x {:a 10}}
        state2 {:x [{:a 10} {:a 20}]}
        cases [[{:a 11} state0 {:path [:a] :op :+ :arg 1}]
               [{:a 9} state0 {:path [:a] :op :- :arg 1}]
               [{:a 20} state0 {:path [:a] :op :* :arg 2}]
               [{:a 5} state0 {:path [:a] :op :/ :arg 2}]
               [{:a 1} state0 {:path [:a] :op :mod :arg 3}]
               [{:x {:a 11}} state1 {:path [:x :a] :op :+ :arg 1}]
               [{:x {:a 9}} state1 {:path [:x :a] :op :- :arg 1}]
               [{:x {:a 30}} state1 {:path [:x :a] :op :* :arg 3}]
               [{:x {:a (/ 10 3)}} state1 {:path [:x :a] :op :/ :arg 3}]
               [{:x {:a 1}} state1 {:path [:x :a] :op :mod :arg 3}]
               [{:x [{:a 10} {:a 21}]} state2 {:path [:x 1 :a] :op :+ :arg 1}]
               [{:x [{:a 10} {:a 19}]} state2 {:path [:x 1 :a] :op :- :arg 1}]]]
    (doseq [case cases]
      (let [[expected state* cmd] case
            ret (commands/eval-cmd state* cmd)]
        (is (= case [ret state* cmd]))))))

(deftest test-ordered-update-maps
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             sub-map '{title [:local :msgs 0 :title]}
             ch (ca/chan 1)
             update-fn #(ca/put! ch (% 'title))
             orig-title "Plato"
             new-title "Socrates"]
         (is (= true (au/<? (vivo/<update-state!
                             vc [{:path [:local]
                                  :op :set
                                  :arg {:msgs [{:title orig-title}]}}]))))
         (vivo/subscribe! vc sub-map nil update-fn "test")
         (is (= orig-title (au/<? ch)))
         (au/<? (vivo/<update-state! vc
                                     [{:path [:local :msgs 0]
                                       :op :insert-before
                                       :arg {:title orig-title}}
                                      {:path [:local :msgs 0 :title]
                                       :op :set
                                       :arg new-title}]))
         (is (= new-title (au/<? ch)))
         (au/<? (vivo/<update-state! vc [{:path [:local :msgs 0 :title]
                                          :op :set
                                          :arg new-title}
                                         {:path [:local :msgs 0]
                                          :op :insert-before
                                          :arg {:title orig-title}}]))
         (is (= orig-title (au/<? ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-end-relative-sub-map
  (au/test-async
   10000
   (ca/go
     (try
       (let [orig-title "Foo"
             vc (vivo/vivo-client)
             ch (ca/chan 1)
             sub-map '{title [:local :msgs -1 :title]}
             update-fn #(ca/put! ch (% 'title))
             new-title "Bar"]
         (au/<? (vivo/<update-state! vc [{:path [:local]
                                          :op :set
                                          :arg {:msgs [{:title orig-title}]}}]))
         (vivo/subscribe! vc sub-map nil update-fn "test")
         (is (= orig-title (au/<? ch)))
         (au/<? (vivo/<update-state! vc [{:path [:local :msgs -1]
                                          :op :insert-after
                                          :arg {:title new-title}}]))
         (is (= new-title (au/<? ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-resolution-map
  (au/test-async
   10000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             ch (ca/chan 1)
             book-id "123"
             book-title "Treasure Island"
             resolution-map {'book-id book-id}
             sub-map '{title [:local :books book-id :title]}
             update-fn #(ca/put! ch (% 'title))]
         (au/<? (vivo/<update-state! vc [{:path [:local :books book-id]
                                          :op :set
                                          :arg {:title book-title}}]))
         (vivo/subscribe! vc sub-map nil update-fn "test" resolution-map)
         (is (= book-title (au/<? ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-sequence-join
  (au/test-async
   10000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             ch (ca/chan 1)
             my-book-ids ["123" "456"]
             resolution-map {'my-book-ids my-book-ids}
             book123 {:title "Treasure Island"}
             book456 {:title "Kidnapped"}
             book789 {:title "Dr Jekyll and Mr Hyde"}
             books {"123" book123
                    "456" book456
                    "789" book789}
             sub-map '{my-books [:local :books my-book-ids]}
             update-fn #(ca/put! ch ('my-books %))
             expected (mapv books my-book-ids)]
         (au/<? (vivo/<update-state! vc [{:path [:local :books]
                                          :op :set
                                          :arg books}]))
         (vivo/subscribe! vc sub-map nil update-fn "test" resolution-map)
         (is (= expected (au/<? ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-relationship
  (are [ret ks1 ks2] (= ret (u/relationship-info ks1 ks2))
    [:equal nil] [] []
    [:equal nil]  [:a] [:a]
    [:equal nil] [:a :b :c] [:a :b :c]
    [:parent [:a]] [] [:a]
    [:parent [:b]] [:a] [:a :b]
    [:parent [:b :c]] [:a] [:a :b :c]
    [:parent [:c]] [:a :b] [:a :b :c]
    [:child nil][:a] []
    [:child nil] [:a :b] [:a]
    [:child nil] [:a :b :c :d] [:a :b]
    [:sibling nil] [:a] [:b]
    [:sibling nil] [:a :c] [:b :c]
    [:sibling nil] [:b] [:c :d :e]
    [:sibling nil] [:a :b] [:a :c]
    [:sibling nil] [:a :b] [:b :c]
    [:sibling nil] [:a :b] [:a :c]
    [:sibling nil] [:a :c :d] [:a :b :d]))

(deftest test-expand-path-1
  (let [path [:b [1 2] :c [3 5] :d]
        expected #{[:b 1 :c 3 :d]
                   [:b 1 :c 5 :d]
                   [:b 2 :c 3 :d]
                   [:b 2 :c 5 :d]}]
    (is (= expected (set (u/expand-path path))))))

(deftest test-expand-path-2
  (let [path [[1 2][3 5] :d]
        expected #{[1 3 :d]
                   [1 5 :d]
                   [2 3 :d]
                   [2 5 :d]}]
    (is (= expected (set (u/expand-path path))))))
