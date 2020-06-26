(ns com.dept24c.unit.vivo-test
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [clojure.test :refer [are deftest is]]
   [com.dept24c.vivo :as vivo]
   [com.dept24c.vivo.client.subscriptions :as subscriptions]
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.macro-impl :as macro-impl]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(u/configure-capsule-logging :debug)

(deftest test-parse-def-component-args-no-docstring
  (let [arglist '[vc a b]
        sub-map '{x [:local :c]}
        args [arglist sub-map]
        expected {:sub-map sub-map
                  :arglist arglist
                  :body nil}]
    (is (= expected (macro-impl/parse-def-component-args 'foo args)))))

(deftest test-parse-def-component-args-with-docstring
  (let [docstring "My component"
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
       (macro-impl/check-arglist 'foo true '[]))))

(deftest test-check-arglist-no-vc-2
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"First argument must be `vc`"
       (macro-impl/check-arglist 'foo true '[x]))))

(deftest test-check-arglist
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"The argument list must be a vector"
       (macro-impl/check-arglist 'foo false '()))))

(deftest test-repeat-symbol
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Illegal repeated symbol"
       (macro-impl/parse-def-component-args
        'foo ['[vc a] '{a [:local :a]}]))))

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
         #"The `sub-map` argument must contain at least one entry"
         (vivo/subscribe! vc "test" bad-sub-map (constantly true))))))

(deftest test-nil-sub-map
  (let [vc (vivo/vivo-client)
        bad-sub-map nil]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"The `sub-map` argument must be a map"
         (vivo/subscribe! vc "test" bad-sub-map (constantly true))))))

(deftest test-non-sym-key-in-sub-map
  (let [vc (vivo/vivo-client)
        bad-sub-map {:not-a-symbol [:local :user-id]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Keys must be symbols"
         (vivo/subscribe! vc "test" bad-sub-map (constantly true))))))

(deftest test-bad-path-in-sub-map
  (let [vc (vivo/vivo-client)
        bad-sub-map '{user-id [:local 8.9]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Only integers"
         (vivo/subscribe! vc "test" bad-sub-map (constantly true))))))

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
       (is (= true
              (au/<? (vivo/<update-state! vc [{:path [:local :users]
                                               :op :set
                                               :arg {user-id {:name name}}}
                                              {:path [:local :user-id]
                                               :op :set
                                               :arg user-id}]))))
       (is (= expected (vivo/subscribe! vc "test" sub-map update-fn)))))))

(deftest test-subscribe!-single-entry
  (au/test-async
   1000
   (ca/go
     (let [vc (vivo/vivo-client)
           update-fn (constantly nil)
           name "Alice"
           user-id "123"
           sub-map '{id [:local :user-id]}]
       (au/<? (vivo/<update-state! vc [{:path [:local :user-id]
                                        :op :set
                                        :arg user-id}]))
       (is (= {'id user-id} (vivo/subscribe! vc test sub-map update-fn)))))))

(deftest test-commands-get-set
  (let [state {:some-stuff [{:name "a"}
                            {:name "b"}]}
        get-path [:local :some-stuff -1 :name]
        get-ret (commands/get-in-state state get-path :local)
        expected-get-ret {:norm-path [:local :some-stuff 1 :name]
                          :val "b"}
        _ (is (= expected-get-ret get-ret))
        set-path [:local :some-stuff -1]
        set-ret (commands/eval-cmd state {:path set-path
                                          :op :set
                                          :arg {:name "new"}}
                                   :local)
        set-expected {:state {:some-stuff [{:name "a"}
                                           {:name "new"}]},
                      :update-info {:norm-path [:local :some-stuff 1]
                                    :op :set
                                    :value {:name "new"}}}]
    (is (= set-expected set-ret))))

(deftest test-insert-after
  (let [state [:a :b]
        path [:local -1]
        arg :new
        ret (commands/insert* state path :local :insert-after arg)
        expected {:state [:a :b :new],
                  :update-info {:norm-path [:local 2]
                                :op :insert-after
                                :value :new}}]
    (is (= expected ret))))

(deftest test-insert-before
  (let [state [:a :b]
        path [:local -1]
        arg :new
        ret (commands/insert* state path :local :insert-before arg)
        expected {:state [:a :new :b],
                  :update-info {:norm-path [:local 1]
                                :op :insert-before
                                :value :new}}]
    (is (= expected ret))))

(deftest test-simple-remove-prefix
  (let [state [:a :b :c]
        path [:local -1]
        ret (commands/eval-cmd state {:path path :op :remove} :local)
        expected {:state [:a :b],
                  :update-info {:norm-path [:local 2]
                                :op :remove
                                :value nil}}]
    (is (= expected ret))))

(deftest test-simple-remove-no-prefix
  (let [state [:a :b :c]
        path [-1]
        ret (commands/eval-cmd state {:path path :op :remove} nil)
        expected {:state [:a :b],
                  :update-info {:norm-path [2]
                                :op :remove
                                :value nil}}]
    (is (= expected ret))))

(deftest test-simple-insert*
  (let [state [:a :b]
        cases [[[:a :b :new] {:path [:local -1]
                              :op :insert-after
                              :arg :new}]
               [[:a :new :b] {:path [:local -1]
                              :op :insert-before
                              :arg :new}]
               [[:new :a :b] {:path [:local 0]
                              :op :insert-before
                              :arg :new}]
               [[:a :new :b] {:path [:local 0]
                              :op :insert-after
                              :arg :new}]
               [[:a :b :new] {:path [:local 1]
                              :op :insert-after
                              :arg :new}]
               [[:a :new :b] {:path [:local 1]
                              :op :insert-before
                              :arg :new}]
               [[:a :b :new] {:path [:local 2]
                              :op :insert-after
                              :arg :new}]
               [[:a :b :new] {:path [:local 10]
                              :op :insert-after
                              :arg :new}]
               [[:new :a :b] {:path [:local -10]
                              :op :insert-after
                              :arg :new}]
               [[:a :b :new] {:path [:local 10]
                              :op :insert-before
                              :arg :new}]
               [[:new :a :b] {:path [:local -10]
                              :op :insert-before
                              :arg :new}]]]
    (doseq [case cases]
      (let [[expected {:keys [path op arg] :as cmd}] case
            ret (:state (commands/insert* state path :local op arg))]
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
                                   :arg :new}]]]
    (doseq [case cases]
      (let [[expected {:keys [path op arg] :as cmd}] case
            ret (:state (commands/insert* state path nil op arg))]
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
         (vivo/subscribe! vc "test" sub-map (constantly nil))))))

(deftest test-bad-path-root-in-sub-map-not-a-sequence
  (let [vc (vivo/vivo-client)
        sub-map '{subject-id :vivo/subject-id}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Paths must be sequences"
         (vivo/subscribe! vc test sub-map (constantly nil))))))

(deftest test-bad-insert*-on-map
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"does not point to a sequence"
       (commands/insert* {:local {}} [:local 0] :local :insert-before :new))))

(deftest test-bad-insert*-path
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"the last element of the path must be an integer"
       (commands/insert* [] [] :local :insert-before :new))))

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
        cases [[{:w 1} {:w 1 :z 2} [:local :z]]
               [{:w 1 :z {:b 2}} {:w 1 :z {:a 1 :b 2}} [:local :z :a]]
               [{:x [:b :c]} state [:local :x 0]]
               [{:x [:a :c]} state [:local :x 1]]
               [{:x [:a :b]} state [:local :x 2]]
               [{:x [:a :b]} state [:local :x -1]]
               [{:x [:a :c]} state [:local :x -2]]]]
    (doseq [case cases]
      (let [[expected state* path] case
            ret (:state (commands/eval-cmd state*
                                           {:path path :op :remove} :local))]
        (is (= case [ret state* path]))))))

(deftest test-math-no-prefix
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
            ret (:state (commands/eval-cmd state* cmd nil))]
        (is (= case [ret state* cmd]))))))

(deftest test-math-local-prefix
  (let [state0 {:a 10}
        state1 {:x {:a 10}}
        state2 {:x [{:a 10} {:a 20}]}
        cases [[{:a 11} state0 {:path [:local :a] :op :+ :arg 1}]
               [{:a 9} state0 {:path [:local :a] :op :- :arg 1}]
               [{:a 20} state0 {:path [:local :a] :op :* :arg 2}]
               [{:a 5} state0 {:path [:local :a] :op :/ :arg 2}]
               [{:a 1} state0 {:path [:local :a] :op :mod :arg 3}]
               [{:x {:a 11}} state1 {:path [:local :x :a] :op :+ :arg 1}]
               [{:x {:a 9}} state1 {:path [:local :x :a] :op :- :arg 1}]
               [{:x {:a 30}} state1 {:path [:local :x :a] :op :* :arg 3}]
               [{:x {:a (/ 10 3)}} state1 {:path [:local :x :a] :op :/ :arg 3}]
               [{:x {:a 1}} state1 {:path [:local :x :a] :op :mod :arg 3}]
               [{:x [{:a 10} {:a 21}]} state2 {:path [:local :x 1 :a] :op :+ :arg 1}]
               [{:x [{:a 10} {:a 19}]} state2 {:path [:local :x 1 :a] :op :- :arg 1}]]]
    (doseq [case cases]
      (let [[expected state* cmd] case
            ret (:state (commands/eval-cmd state* cmd :local))]
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
         (is (= {'title orig-title}
                (vivo/subscribe! vc "test" sub-map update-fn)))
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
   1000
   (ca/go
     (try
       (let [orig-title "Foo"
             vc (vivo/vivo-client)
             ch (ca/chan 1)
             sub-map '{last-title [:local :msgs -1 :title]}
             update-fn #(ca/put! ch (% 'last-title))
             new-title "Bar"
             ret (au/<? (vivo/<update-state!
                         vc [{:path [:local]
                              :op :set
                              :arg {:msgs [{:title orig-title}]}}]))]
         (is (= true ret))
         (is (= {'last-title orig-title}
                (vivo/subscribe! vc "test" sub-map update-fn)))
         (au/<? (vivo/<update-state! vc [{:path [:local :msgs -1]
                                          :op :insert-after
                                          :arg {:title new-title}}]))
         (is (= new-title (au/<? ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-resolution-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             book-id "123"
             book-title "Treasure Island"
             resolution-map {'book-id book-id}
             sub-map '{title [:local :books book-id :title]}
             update-fn (constantly nil)]
         (is (= true
                (au/<? (vivo/<update-state! vc [{:path [:local :books book-id]
                                                 :op :set
                                                 :arg {:title book-title}}]))))
         (is (= {'title book-title}
                (vivo/subscribe! vc "test" sub-map update-fn
                                 (u/sym-map resolution-map)))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-symbolic-path
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             book-id "123"
             book-title "Treasure Island"
             resolution-map {'d-path [:local :books 'the-id :title]}
             sub-map '{the-id [:local :the-id]
                       title d-path}
             update-fn (constantly nil)]
         (is (= true
                (au/<? (vivo/<update-state! vc [{:path [:local :the-id]
                                                 :op :set
                                                 :arg book-id}
                                                {:path [:local :books book-id]
                                                 :op :set
                                                 :arg {:title book-title}}]))))
         (is (= '{the-id "123"
                  title "Treasure Island"}
                (vivo/subscribe! vc "test" sub-map update-fn
                                 (u/sym-map resolution-map)))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-sequence-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             my-book-ids ["123" "456"]
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-books [:local :books my-book-ids]}
             resolution-map {'my-book-ids my-book-ids}
             update-fn (constantly nil)
             expected {'my-books (vals (select-keys books my-book-ids))}]
         (is (= true (au/<? (vivo/<update-state! vc [{:path [:local :books]
                                                      :op :set
                                                      :arg books}]))))
         (is (= expected (vivo/subscribe! vc "test" sub-map update-fn
                                          (u/sym-map resolution-map)))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-sequence-join-in-sub-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             update-ch (ca/chan 1)
             my-book-ids ["123" "456"]
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-book-ids [:local :my-book-ids]
                       my-books [:local :books my-book-ids]}
             update-fn #(ca/put! update-ch %)
             expected {'my-book-ids my-book-ids
                       'my-books (vals (select-keys books my-book-ids))}]
         (is (= true (au/<? (vivo/<update-state!
                             vc [{:path [:local :books]
                                  :op :set
                                  :arg books}
                                 {:path [:local :my-book-ids]
                                  :op :set
                                  :arg my-book-ids}]))))
         (is (= expected (vivo/subscribe! vc "test" sub-map update-fn)))
         (is (= true (au/<? (vivo/<update-state!
                             vc [{:path [:local :my-book-ids 0]
                                  :op :remove}]))))
         (is (= {'my-book-ids ["456"]
                 'my-books [{:title "Kidnapped"}]} (au/<? update-ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-sequence-join-in-sub-map-2
  ;; Tests changing state whose path depends on a different path in sub-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             update-ch (ca/chan 1)
             my-book-ids ["123" "789"]
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-book-ids [:local :my-book-ids]
                       my-books [:local :books my-book-ids]}
             update-fn #(ca/put! update-ch %)
             new-title "Strange Case of Dr Jekyll and Mr Hyde"
             expected1 {'my-book-ids my-book-ids
                        'my-books [{:title "Treasure Island"}
                                   {:title "Dr Jekyll and Mr Hyde"}]}
             expected2 {'my-book-ids my-book-ids
                        'my-books [{:title "Treasure Island"}
                                   {:title new-title}]}]
         (is (= true (au/<? (vivo/<update-state!
                             vc [{:path [:local :books]
                                  :op :set
                                  :arg books}
                                 {:path [:local :my-book-ids]
                                  :op :set
                                  :arg my-book-ids}]))))
         (is (= expected1 (vivo/subscribe! vc "test" sub-map update-fn)))
         (is (= true (au/<? (vivo/<update-state!
                             vc [{:path [:local :books "789" :title]
                                  :op :set
                                  :arg new-title}]))))
         (is (= expected2 (au/<? update-ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-set-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             ch (ca/chan 1)
             my-book-ids #{"123" "789"}
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-books [:local :books my-book-ids]}
             resolution-map {'my-book-ids my-book-ids}
             update-fn #(ca/put! ch %)
             expected {'my-books (vals (select-keys books my-book-ids))}]
         (is (= {'my-books [nil nil]}
                (vivo/subscribe! vc "test" sub-map update-fn
                                 (u/sym-map resolution-map))))
         (is (= true (au/<? (vivo/<update-state! vc [{:path [:local :books]
                                                      :op :set
                                                      :arg books}]))))
         (is (= expected (au/<? ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-kw-operators
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             ch (ca/chan 1)
             books {"123" {:title "Treasure Island" :nums [2 4 6]}
                    "456" {:title "Kidnapped" :nums [1 3]}
                    "789" {:title "Dr Jekyll and Mr Hyde" :nums [5 7]}}
             msgs [{:text "hi" :user-id "123"}
                   {:text "there" :user-id "123"}]
             titles-set (set (map :title (vals books)))
             sub-map '{books-map [:local :books]
                       books-vals [:local :books :vivo/*]
                       titles-1 [:local :books :vivo/* :title]
                       book-ids [:local :books :vivo/keys]
                       titles-2 [:local :books book-ids :title]
                       num-books [:local :books :vivo/count]
                       num-books-2 [:local :books :vivo/* :vivo/count]
                       num-msgs [:local :msgs :vivo/count]
                       book-nums [:local :books :vivo/* :nums :vivo/concat]
                       msgs [:local :msgs]
                       msg-indices [:local :msgs :vivo/keys]}
             update-fn (constantly nil)
             expected {'book-ids #{"123" "456" "789"}
                       'books-map books
                       'books-vals (set (vals books))
                       'num-books (count books)
                       'num-books-2 (count books)
                       'book-nums #{2 4 6 1 3 5 7}
                       'titles-1 titles-set
                       'titles-2 titles-set
                       'num-msgs 2
                       'msg-indices [0 1]
                       'msgs msgs}
             update-ret (au/<? (vivo/<update-state!
                                vc [{:path [:local]
                                     :op :set
                                     :arg (u/sym-map books msgs)}]))
             sub-ret (vivo/subscribe! vc "test" sub-map update-fn)]
         (is (= true update-ret))
         (is (= expected
                (-> sub-ret
                    (update 'book-ids set)
                    (update 'books-vals set)
                    (update 'book-nums set)
                    (update 'titles-1 set)
                    (update 'titles-2 set)))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e))
         (println (u/ex-msg-and-stacktrace e)))))))

(deftest test-empty-sequence-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-book-ids [:local :my-book-ids]
                       my-books [:local :books my-book-ids]}
             update-fn (constantly nil)
             expected '{my-book-ids []
                        my-books nil}]
         (au/<? (vivo/<update-state! vc [{:path [:local :my-book-ids]
                                          :op :set
                                          :arg []}
                                         {:path [:local :books]
                                          :op :set
                                          :arg books}]))
         (is (= expected (vivo/subscribe! vc "test" sub-map update-fn))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-sub-map->map-info
  (let [sub-map '{c [:local :b-to-c b]
                  b [:local :a-to-b a]
                  a [:local :a]
                  x [:local x]
                  z dynamic-path}
        resolution-map '{x :foo
                         dynamic-path [:local :dp]}
        info (u/sub-map->map-info sub-map resolution-map)
        {:keys [independent-pairs ordered-dependent-pairs]} info
        expected-independent-pairs-set (set '[[x [:local :foo]]
                                              [a [:local :a]]
                                              [z [:local :dp]]]),
        expected-ordered-dependent-pairs '[[b [:local :a-to-b a]]
                                           [c [:local :b-to-c b]]]]
    (is (= expected-ordered-dependent-pairs ordered-dependent-pairs))
    (is (= expected-independent-pairs-set
           (set independent-pairs)))))

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

(defn <mock-ks-at-path [path]
  (au/go
    nil))

(deftest test-expand-path-1
  (au/test-async
   1000
   (ca/go
     (try
       (let [path [:x [:a :b]]
             expected [[:x :a]
                       [:x :b]]]
         (is (= expected (au/<? (u/<expand-path <mock-ks-at-path path)))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-expand-path-2
  (au/test-async
   1000
   (ca/go
     (try
       (let [path [:b [1 2] :c [3 5] :d]
             expected [[:b 1 :c 3 :d]
                       [:b 2 :c 3 :d]
                       [:b 1 :c 5 :d]
                       [:b 2 :c 5 :d]]]
         (is (= expected (au/<? (u/<expand-path <mock-ks-at-path path)))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-expand-path-3
  (au/test-async
   1000
   (ca/go
     (try
       (let [path [[1 2][3 5] :d]
             expected [[1 3 :d]
                       [2 3 :d]
                       [1 5 :d]
                       [2 5 :d]]]
         (is (= expected (au/<? (u/<expand-path <mock-ks-at-path path)))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-expand-path-4
  (au/test-async
   1000
   (ca/go
     (try
       (let [path [:x ["1" "2"] :y [:a :b]]
             expected
             [[:x "1" :y :a]
              [:x "2" :y :a]
              [:x "1" :y :b]
              [:x "2" :y :b]]]
         (is (= expected (au/<? (u/<expand-path <mock-ks-at-path path)))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-update-sub?-numeric
  (let [update-infos [{:norm-path [:sys 0]
                       :op :insert
                       :value "hi"}]
        sub-paths [[:local :page]]]
    (is (= false (subscriptions/update-sub? update-infos sub-paths)))))

(deftest test-order-by-lineage
  (let [*name->info (atom {"a" {}
                           "b" {:parents #{"a"}}
                           "c" {:parents #{"a" "b"}}
                           "d" {:parents #{"a"}}
                           "e" {}
                           "f" {:parents #{"e"}}
                           "g" {:parents #{"e"}}
                           "h" {:parents #{"e" "g"}}})]
    (is (= ["a"] (subscriptions/order-by-lineage
                  #{"a"} *name->info)))
    (is (= ["a" "b"] (subscriptions/order-by-lineage
                      #{"a" "b"} *name->info)))
    (is (= ["a" "b" "c"] (subscriptions/order-by-lineage
                          #{"a" "b" "c"} *name->info)))
    (is (= ["b" "c"] (subscriptions/order-by-lineage
                      #{"b" "c"} *name->info)))
    (is (= ["a" "b" "c" "d"] (subscriptions/order-by-lineage
                              #{"a" "b" "c" "d"} *name->info)))
    (is (= ["a" "b" "c" "e" "d"] (subscriptions/order-by-lineage
                                  #{"a" "b" "c" "d" "e"} *name->info)))
    (is (= ["e"] (subscriptions/order-by-lineage
                  #{"e"} *name->info)))
    (is (= ["e" "f"] (subscriptions/order-by-lineage
                      #{"e" "f"} *name->info)))
    (is (= ["h" "f"] (subscriptions/order-by-lineage
                      #{"h" "f"} *name->info)))))

(deftest test-explicit-seq-in-path
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-titles [:local :books ["123" "789"] :title]}
             update-fn (constantly nil)
             expected {'my-titles ["Treasure Island" "Dr Jekyll and Mr Hyde"]}]
         (au/<? (vivo/<set-state! vc [:local :books] books))
         (is (= expected (vivo/subscribe! vc "test" sub-map update-fn))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{title-999 [:local :books "999" :title]}
             update-fn (constantly nil)
             ret (au/<? (vivo/<set-state! vc [:local :books] books))]
         (is (= true ret))
         (is (= {'title-999 nil}
                (vivo/subscribe! vc "test" sub-map update-fn))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-seq-w-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-titles [:local :books ["999"] :title]}
             update-fn (constantly nil)
             expected {'my-titles [nil]}]
         (au/<? (vivo/<set-state! vc [:local :books] books))
         (is (= expected (vivo/subscribe! vc "test" sub-map update-fn))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-wildcard-sub
  (au/test-async
   3000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             ch (ca/chan 1)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             titles-set (set (map :title (vals books)))
             sub-map '{titles [:local :books :vivo/* :title]}
             update-fn #(ca/put! ch (update % 'titles set))
             ret0 (vivo/subscribe! vc "test" sub-map update-fn)
             _ (is (= {'titles []} ret0))
             ret1 (au/<? (vivo/<set-state! vc [:local :books] books))
             _ (is (= true ret1))
             _ (is (= {'titles titles-set} (au/<? ch)))
             ret2 (au/<? (vivo/<update-state! vc [{:path [:local :books "999"]
                                                   :op :set
                                                   :arg {:title "1984"}}]))
             _ (is (= true ret2))
             _ (is (= {'titles (conj titles-set "1984")} (au/<? ch)))
             ret3 (au/<? (vivo/<update-state! vc [{:path [:local :books "456"]
                                                   :op :remove}]))
             _ (is (= true ret3))
             expected-titles (-> (conj titles-set "1984")
                                 (disj "Kidnapped"))
             _ (is (= {'titles expected-titles} (au/<? ch)))])
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-array-ops
  (au/test-async
   3000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             ch (ca/chan 1)
             id-to-fav-nums {1 [7 8 9]
                             2 [2 3 4]}
             ret1 (au/<? (vivo/<set-state! vc
                                           [:local :id-to-fav-nums]
                                           id-to-fav-nums))
             _ (is (= true ret1))
             resolution-map {'id 2}
             sub-map '{my-nums [:local :id-to-fav-nums id]}
             update-fn #(ca/put! ch %)
             ret2 (vivo/subscribe! vc "test" sub-map update-fn
                                   (u/sym-map resolution-map))
             _ (is (= {'my-nums [2 3 4]} ret2))
             ret3 (au/<? (vivo/<update-state!
                          vc [{:path [:local :id-to-fav-nums 2 1]
                               :op :remove}]))
             _ (is (= true ret3))
             _ (is (= {'my-nums [2 4]} (au/<? ch)))
             ret4 (au/<? (vivo/<update-state!
                          vc [{:path [:local :id-to-fav-nums 2 -1]
                               :op :insert-after
                               :arg 5}]))
             _ (is (= true ret4))
             _ (is (= {'my-nums [2 4 5]} (au/<? ch)))])
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-get-synchronous-state-and-expanded-paths
  (let [*subject-id (atom nil)
        independent-pairs [['page [:local :page]]
                           ['subject-id [:vivo/subject-id]]]
        ordered-dependent-pairs []
        db {}
        local-state {:page :frobnozzle}
        ret (subscriptions/get-synchronous-state-and-expanded-paths
             independent-pairs ordered-dependent-pairs
             db local-state *subject-id)
        {:keys [state expanded-paths]} ret
        expected-state {'page :frobnozzle
                        'subject-id nil}
        expected-expanded-paths [[:local :page]
                                 [:vivo/subject-id]]
        _ (is (= expected-state state))
        _ (is (= expected-expanded-paths expanded-paths))
        _ (reset! *subject-id "AAAA")
        ret (subscriptions/get-synchronous-state-and-expanded-paths
             independent-pairs ordered-dependent-pairs
             db local-state *subject-id)
        {:keys [state expanded-paths]} ret
        expected-state {'page :frobnozzle
                        'subject-id "AAAA"}
        expected-expanded-paths [[:local :page]
                                 [:vivo/subject-id]]
        _ (is (= expected-state state))
        _ (is (= expected-expanded-paths expanded-paths))]))
