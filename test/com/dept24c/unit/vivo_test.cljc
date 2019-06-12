(ns com.dept24c.unit.vivo-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [are deftest is]]
   [com.dept24c.vivo :as vivo]
   [com.dept24c.vivo.macro-impl :as macro-impl]
   [com.dept24c.vivo.state :as state]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-parse-def-component-args-sequential
  (let [arglist '[sm a b]
        args [arglist]
        expected [nil arglist nil]]
    (is (= expected (macro-impl/parse-def-component-args 'foo args)))))

(deftest test-parse-def-component-args-map
  (let [sub-map '{x [:b :c]}
        arglist '[sm a b]
        args [sub-map arglist]
        expected [sub-map arglist nil]]
    (is (= expected (macro-impl/parse-def-component-args 'foo args)))))

(deftest test-parse-def-component-args-bad-arg
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Bad argument to component"
       (macro-impl/parse-def-component-args 'foo nil))))

(deftest test-check-arglist-no-sm
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"First argument must be `sm`"
       (macro-impl/check-arglist 'foo '[]))))

(deftest test-check-arglist-no-sm-2
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"First argument must be `sm`"
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
       (macro-impl/parse-def-component-args 'foo ['{a [:b]} '[sm a]]))))

(deftest test-check-sub-map-undefined-sym
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Undefined symbol\(s\) in subscription map"
       (u/check-sub-map 'foo "component" '{a [:b c]}))))

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
  (let [sm (vivo/state-manager)
        bad-sub-map {}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"The sub-map parameter must contain at least one entry"
         (vivo/subscribe! sm "test-1" bad-sub-map (constantly true))))))

(deftest test-nil-sub-map
  (let [sm (vivo/state-manager)
        bad-sub-map nil]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"The sub-map parameter must be a map"
         (vivo/subscribe! sm "test-1" bad-sub-map (constantly true))))))

(deftest test-non-sym-key-in-sub-map
  (let [sm (vivo/state-manager)
        bad-sub-map {:not-a-symbol [:local :user-id]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"All keys in sub-map must be symbols. Got `:not-a-symbol`"
         (vivo/subscribe! sm "test-1" bad-sub-map (constantly true))))))

(deftest test-bad-path-in-sub-map
  (let [sm (vivo/state-manager)
        bad-sub-map '{user-id [:local nil]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Only integers, keywords, symbols, and strings are valid path keys"
         (vivo/subscribe! sm "test-1" bad-sub-map (constantly true))))))

(deftest test-bad-symbol-in-sub-map
  (let [sm (vivo/state-manager)
        bad-sub-map '{user-id [:local a-symbol-which-is-not-defined]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Undefined symbol.* in subscription map"
         (vivo/subscribe! sm "test-1" bad-sub-map (constantly true))))))

(deftest test-subscribe!
  (au/test-async
   1000
   (ca/go
     (let [sm (vivo/state-manager)
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           name "Alice"
           user-id "123"
           sub-map '{id [:local :user-id]
                     name [:local :users id :name]
                     tx-info :vivo/tx-info}
           expected '{id "123"
                      name "Alice"
                      tx-info :initial-subscription}]
       (au/<? (vivo/<update-state! sm [{:path [:local :users]
                                        :op :set
                                        :arg {user-id {:name name}}}
                                       {:path [:local :user-id]
                                        :op :set
                                        :arg user-id}]))
       (vivo/subscribe! sm "test-1" sub-map update-fn)
       (is (= expected (au/<? ch)))))))

(deftest test-subscribe!-single-entry
  (au/test-async
   1000
   (ca/go
     (let [sm (vivo/state-manager)
           ch (ca/chan 1)
           update-fn #(ca/put! ch (% 'id))
           name "Alice"
           user-id "123"
           sub-map '{id [:local :user-id]}]
       (au/<? (vivo/<update-state! sm [{:path [:local :user-id]
                                        :op :set
                                        :arg user-id}]))
       (vivo/subscribe! sm "test-1" sub-map update-fn)
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
            ret (state/insert* state path op arg)]
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
            ret (state/insert* state path op arg)]
        (is (= case [ret cmd]))))))

(deftest test-bad-path-root-in-update-state!
  (let [sm (vivo/state-manager)]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Paths must begin with either :local or :sys. Got `:not-a-valid-root`"
         (vivo/update-state! sm [{:path [:not-a-valid-root :x]
                                  :op :set
                                  :arg 1}])))))

(deftest test-bad-path-root-in-sub-map
  (let [sm (vivo/state-manager)
        sub-map '{a [:not-a-valid-root :x]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Paths must begin with either :local or :sys. Got `:not-a-valid-root`"
         (vivo/subscribe! sm "sub123" sub-map (constantly nil))))))

(deftest test-bad-insert*-on-map
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"does not point to a vector"
       (state/insert* {} [0] :insert-before :new))))

(deftest test-bad-insert*-path
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"the last element of the path must be an integer"
       (state/insert* [] [] :insert-before :new))))

(deftest test-bad-command-op
  (let [sm (vivo/state-manager)]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"is not a valid op. Got: `:not-an-op`."
         (vivo/update-state! sm [{:path [:local :x]
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
            ret (state/eval-cmd state* {:path path :op :remove})]
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
            ret (state/eval-cmd state* cmd)]
        (is (= case [ret state* cmd]))))))

(deftest test-ordered-update-maps
  (au/test-async
   1000
   (ca/go
     (let [sm (vivo/state-manager)
           sub-map '{title [:local :msgs 0 :title]}
           ch (ca/chan 1)
           update-fn #(ca/put! ch (% 'title))
           orig-title "Plato"
           new-title "Socrates"]
       (au/<? (vivo/<update-state! sm [{:path [:local]
                                        :op :set
                                        :arg {:msgs [{:title orig-title}]}}]))
       (vivo/subscribe! sm "test-1" sub-map update-fn)
       (is (= orig-title (au/<? ch)))
       (vivo/update-state! sm
                           [{:path [:local :msgs 0]
                             :op :insert-before
                             :arg {:title orig-title}}
                            {:path [:local :msgs 0 :title]
                             :op :set
                             :arg new-title}])
       (is (= new-title (au/<? ch)))
       (vivo/update-state! sm [{:path [:local :msgs 0 :title]
                                :op :set
                                :arg new-title}
                               {:path [:local :msgs 0]
                                :op :insert-before
                                :arg {:title orig-title}}])
       (is (= orig-title (au/<? ch)))))))

(deftest test-end-relative-sub-map
  (au/test-async
   10000
   (ca/go
     (let [orig-title "Foo"
           sm (vivo/state-manager)
           ch (ca/chan 1)
           sub-map '{title [:local :msgs -1 :title]}
           update-fn #(ca/put! ch (% 'title))
           new-title "Bar"]
       (au/<? (vivo/<update-state! sm [{:path [:local]
                                        :op :set
                                        :arg {:msgs [{:title orig-title}]}}]))
       (vivo/subscribe! sm "test-1" sub-map update-fn)
       (is (= orig-title (au/<? ch)))
       (vivo/update-state! sm [{:path [:local :msgs -1]
                                :op :insert-after
                                :arg {:title new-title}}])
       (is (= new-title (au/<? ch)))))))
