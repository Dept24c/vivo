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
        expected {:sub-map sub-map
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
        bad-sub-map '{user-id [:local 8.9]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Only integers, keywords, symbols, and strings are valid path keys"
         (vivo/subscribe! vc bad-sub-map nil (constantly true) "test")))))

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
         (vivo/subscribe! vc sub-map nil (constantly nil) "test")))))

(deftest test-bad-insert*-on-map
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"does not point to a vector"
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
   1000
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
   1000
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
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             ch (ca/chan 1)
             my-book-ids ["123" "456"]
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-books [:local :books my-book-ids]}
             resolution-map {'my-book-ids my-book-ids}
             update-fn #(ca/put! ch ('my-books %))
             expected (vals (select-keys books my-book-ids))]
         (au/<? (vivo/<update-state! vc [{:path [:local :books]
                                          :op :set
                                          :arg books}]))
         (vivo/subscribe! vc sub-map nil update-fn "test" resolution-map)
         (is (= expected (au/<? ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-empty-sequence-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             ch (ca/chan 1)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-book-ids [:local :my-book-ids]
                       my-books [:local :books my-book-ids]}
             update-fn #(ca/put! ch %)
             expected '{my-book-ids []
                        my-books nil}]
         (au/<? (vivo/<update-state! vc [{:path [:local :my-book-ids]
                                          :op :set
                                          :arg []}
                                         {:path [:local :books]
                                          :op :set
                                          :arg books}]))
         (vivo/subscribe! vc sub-map nil update-fn "test")
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
  (let [path [:x [:a :b]]
        expected [[:x :a]
                  [:x :b]]]
    (is (= expected (u/expand-path path)))))

(deftest test-expand-path-2
  (let [path [:b [1 2] :c [3 5] :d]
        expected [[:b 1 :c 3 :d]
                  [:b 2 :c 3 :d]
                  [:b 1 :c 5 :d]
                  [:b 2 :c 5 :d]]]
    (is (= expected (u/expand-path path)))))

(deftest test-expand-path-3
  (let [path [[1 2][3 5] :d]
        expected [[1 3 :d]
                  [2 3 :d]
                  [1 5 :d]
                  [2 5 :d]]]
    (is (= expected (u/expand-path path)))))

(deftest test-expand-path-4
  (let [path [:x ["1" "2"] :y [:a :b]]
        expected
        [[:x "1" :y :a]
         [:x "2" :y :a]
         [:x "1" :y :b]
         [:x "2" :y :b]]]
    (is (= expected (u/expand-path path)))))

(deftest test-update-sub?-numeric
  (let [update-infos [{:norm-path [:sys 0]
                       :op :insert
                       :value "hi"}]
        sub-paths [[:local :page]]]
    (is (= false (u/update-sub? update-infos sub-paths)))))

(deftest test-explicit-seq-in-path
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             ch (ca/chan 1)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-titles [:local :books ["123" "789"] :title]}
             update-fn #(ca/put! ch %)
             expected {'my-titles ["Treasure Island" "Dr Jekyll and Mr Hyde"]}]
         (au/<? (vivo/<set-state! vc [:local :books] books))
         (vivo/subscribe! vc sub-map nil update-fn "test")
         (is (= expected (au/<? ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             ch (ca/chan 1)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{title-999 [:local :books "999" :title]}
             update-fn #(ca/put! ch %)
             expected {'title-999 nil}]
         (au/<? (vivo/<set-state! vc [:local :books] books))
         (vivo/subscribe! vc sub-map nil update-fn "test")
         (is (= expected (au/<? ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-seq-w-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [vc (vivo/vivo-client)
             ch (ca/chan 1)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-titles [:local :books ["999"] :title]}
             update-fn #(ca/put! ch %)
             expected {'my-titles [nil]}]
         (au/<? (vivo/<set-state! vc [:local :books] books))
         (vivo/subscribe! vc sub-map nil update-fn "test")
         (is (= expected (au/<? ch))))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))
