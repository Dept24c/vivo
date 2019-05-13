(ns com.dept24c.unit.vivo-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [are deftest is]]
   [com.dept24c.vivo :as vivo]
   [com.dept24c.vivo.macro-impl :as macro-impl]
   [com.dept24c.vivo.mem-state-provider-impl :as mspi]
   [com.dept24c.vivo.state-manager-impl :as smi]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-sub-map-circular-dependency
  (let [sub-map {'lang [:local :lang 'page]
                 'page [:local :page 'lang]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Circular dependency in subscription map. page and lang are mutually"
         (smi/make-df-key->depend-info sub-map)))))

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

(deftest test-parse-def-component-args-no-sm
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"First argument must be `sm`"
       (macro-impl/parse-def-component-args 'foo [{} []]))))

(deftest test-parse-def-component-args-no-sm-2
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"First argument must be `sm`"
       (macro-impl/parse-def-component-args 'foo [{} '[x]]))))

(deftest test-check-arglist
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"The argument list must be a vector"
       (macro-impl/check-arglist 'foo '()))))

(deftest test-check-subscription-args-repeat-sym
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Illegal repeated symbol"
       (macro-impl/check-subscription-args
        'foo "component" '{a [:b]} '[sm a]))))

(deftest test-check-subscription-args-undefined-sym
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Undefined symbol\(s\) in subscription map"
       (macro-impl/check-subscription-args
        'foo "component" '{a [:b c]} '[sm]))))

(deftest test-check-constructor-args
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Wrong number of arguments .+ Should have gotten 3 .+ got 1"
       (macro-impl/check-constructor-args "foo" [1] 3))))

(deftest test-relationship-info
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

(vivo/def-subscriber name-ch-subscriber
  {name [:local :name]}
  [sm ch]
  (ca/put! ch name))

(deftest test-def-subscriber
  (au/test-async
   1000
   (ca/go
     (let [sm (vivo/state-manager {:local (vivo/mem-state-provider)})
           ch (ca/chan 1)
           sub (name-ch-subscriber sm ch)
           name "Alice"]
       (vivo/update-state! sm {[:local :name] [:set name]})
       (is (= name (au/<? ch)))))))

(deftest test-edn<->str
  (are [edn] (= edn (u/str->edn (u/edn->str edn)))
    "string"
    {:a 1 :b 2}
    [7 :v :x "str"]
    {"s" 1 :s 2}))

(deftest test-update-array-sub?
  (let [cases [[true 10 0 0 :set]
               [false 10 0 0 :insert-after]
               [true 10 0 0 :insert-before]
               [false 10 0 1 :set]
               [false 10 0 1 :insert-after]
               [false 10 0 1 :insert-before]
               [false 10 1 0 :set]
               [true 10 1 0 :insert-after]
               [true 10 1 0 :insert-before]
               [true 10 -1 -1 :set]
               [true 10 -1 -1 :insert-after]
               [false 10 -1 -1 :insert-before]
               [false 10 -2 -1 :set]
               [true 10 -2 -1 :insert-after]
               [true 10 -2 -1 :insert-before]
               [false 10 -1 -2 :set]
               [false 10 -1 -2 :insert-after]
               [false 10 -1 -2 :insert-before]
               [false 10 -1 0 :set]
               [false 10 -1 0 :insert-after]
               [false 10 -1 0 :insert-before]
               [true 1 -1 0 :set]
               [true 1 -1 0 :insert-after]
               [false 1 -1 0 :insert-before]
               [true 0 -1 -1 :insert-after]
               [true 0 -1 0 :insert-before]
               [true 0 0 -1 :insert-after]
               [true 0 0 0 :insert-before]]]
    (doseq [case cases]
      (let [[expected len sub-i update-i op] case
            ret (u/update-array-sub? len sub-i update-i op)]
        (is (= case [ret len sub-i update-i op]))))))

(deftest test-simple-insert*
  (let [state [:a :b]
        cases [[[:a :b :new] [-1] [:insert-after :new]]
               [[:a :new :b] [-1] [:insert-before :new]]
               [[:new :a :b] [0] [:insert-before :new]]
               [[:a :new :b] [0] [:insert-after :new]]
               [[:a :b :new] [1] [:insert-after :new]]
               [[:a :new :b] [1] [:insert-before :new]]
               [[:a :b :new] [2] [:insert-after :new]]
               [[:a :b :new] [10] [:insert-after :new]]
               [[:new :a :b] [-10] [:insert-after :new]]
               [[:a :b :new] [10] [:insert-before :new]]
               [[:new :a :b] [-10] [:insert-before :new]]]]
    (doseq [case cases]
      (let [[expected path upex] case
            ret (mspi/insert* state path upex)]
        (is (= case [ret path upex]))))))

(deftest test-deep-insert*
  (let [state {:x [:a :b]}
        cases [[{:x [:a :b :new]} [:x -1] [:insert-after :new]]
               [{:x [:a :new :b]} [:x -1] [:insert-before :new]]
               [{:x [:new :a :b]} [:x 0] [:insert-before :new]]
               [{:x [:a :new :b]} [:x 0] [:insert-after :new]]
               [{:x [:a :b :new]} [:x 1] [:insert-after :new]]
               [{:x [:a :new :b]} [:x 1] [:insert-before :new]]
               [{:x [:a :b :new]} [:x 2] [:insert-after :new]]
               [{:x [:a :b :new]} [:x 10] [:insert-after :new]]
               [{:x [:new :a :b]} [:x -10] [:insert-after :new]]
               [{:x [:a :b :new]} [:x 10] [:insert-before :new]]
               [{:x [:new :a :b]} [:x -10] [:insert-before :new]]
               [{:x [:a :b] :y [:new]} [:y 0] [:insert-after :new]]
               [{:x [:a :b] :y [:new]} [:y 0] [:insert-before :new]]
               [{:x [:a :b] :y [:new]} [:y -1] [:insert-before :new]]
               [{:x [:a :b] :y [:new]} [:y -1] [:insert-after :new]]]]
    (doseq [case cases]
      (let [[expected path upex] case
            ret (mspi/insert* state path upex)]
        (is (= case [ret path upex]))))))

(deftest test-bad-path-root-in-update-state!
  (let [sm (vivo/state-manager
            {:local (vivo/mem-state-provider {})})]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"No state provider exists for root"
         (vivo/update-state! sm {[:not-a-valid-root :x] [:set 1]})))))

(deftest test-bad-path-root-in-sub-map
  (let [sm (vivo/state-manager {:local (vivo/mem-state-provider)})
        sub-map '{a [:not-a-valid-root x]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"No state provider exists for root"
         (vivo/subscribe! sm "sub123" sub-map (constantly nil))))))

(deftest test-bad-insert*-on-map
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"does not point to a vector"
       (mspi/insert* {} [0] [:insert-before :new]))))

(deftest test-bad-insert*-path
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"the last element of the path must be an integer"
       (mspi/insert* [] [] [:insert-before :new]))))

(deftest test-bad-upex-op
  (let [sm (vivo/state-manager {:m (vivo/mem-state-provider)})]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Invalid operator `:not-an-op`"
         (vivo/update-state!
          sm {[:m :x] [:not-an-op 1]})))))

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
            ret (mspi/eval-upex state* path [:remove])]
        (is (= case [ret state* path]))))))

(deftest test-math
  (let [state0 {:a 10}
        state1 {:x {:a 10}}
        state2 {:x [{:a 10} {:a 20}]}
        cases [[{:a 11} state0 [:a] [:+ 1]]
               [{:a 9} state0 [:a] [:- 1]]
               [{:a 20} state0 [:a] [:* 2]]
               [{:a 5} state0 [:a] [:/ 2]]
               [{:a 1} state0 [:a] [:mod 3]]
               [{:x {:a 11}} state1 [:x :a] [:+ 1]]
               [{:x {:a 9}} state1 [:x :a] [:- 1]]
               [{:x {:a 30}} state1 [:x :a] [:* 3]]
               [{:x {:a (/ 10 3)}} state1 [:x :a] [:/ 3]]
               [{:x {:a 1}} state1 [:x :a] [:mod 3]]
               [{:x [{:a 10} {:a 21}]} state2 [:x 1 :a] [:+ 1]]
               [{:x [{:a 10} {:a 19}]} state2 [:x 1 :a] [:- 1]]]]
    (doseq [case cases]
      (let [[expected state* path upex] case
            ret (mspi/eval-upex state* path upex)]
        (is (= case [ret state* path upex]))))))

(vivo/def-subscriber first-msg-title-subscriber
  {title [:m :msgs 0 :title]}
  [sm ch]
  (ca/put! ch title))

(deftest test-ordered-update-maps
  (au/test-async
   1000
   (ca/go
     (let [orig-title "Plato"
           sm (vivo/state-manager {:m (vivo/mem-state-provider
                                       {:msgs [{:title orig-title}]})})
           ch (ca/chan 1)
           sub (first-msg-title-subscriber sm ch)
           new-title "Socrates"]
       (is (= orig-title (au/<? ch)))
       (vivo/update-state!
        sm [[[:m :msgs 0] [:insert-before {:title orig-title}]]
            [[:m :msgs 0 :title] [:set new-title]]])

       (is (= new-title (au/<? ch)))
       (vivo/update-state!
        sm [[[:m :msgs 0 :title] [:set new-title]]
            [[:m :msgs 0] [:insert-before {:title orig-title}]]])
       (is (= orig-title (au/<? ch)))))))

(vivo/def-subscriber last-msg-title-subscriber
  {title [:m :msgs -1 :title]}
  [sm ch]
  (ca/put! ch title))

(deftest test-end-relative-sub-map
  (au/test-async
   1000
   (ca/go
     (let [orig-title "Foo"
           sm (vivo/state-manager {:m (vivo/mem-state-provider
                                       {:msgs [{:title orig-title}]})})
           ch (ca/chan 1)
           sub (last-msg-title-subscriber sm ch)
           new-title "Bar"]
       (is (= orig-title (au/<? ch)))
       (vivo/update-state!
        sm {[:m :msgs -1] [:insert-after {:title new-title}]})
       (is (= new-title (au/<? ch)))))))

(deftest test-tx-info
  (au/test-async
   1000
   (ca/go
     (let [sm (vivo/state-manager {:m (vivo/mem-state-provider {:a 1})})
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           sub-map '{a [:m :a]
                     my-tx-info :vivo/tx-info}
           tx-info {:some "info"}]
       (vivo/subscribe! sm "sub123" sub-map update-fn)
       (is (= {:a 1} (au/<? ch)))
       (vivo/update-state! sm {[:m :a] [:set 2]} tx-info)
       (is (= {:a 2, :my-tx-info tx-info} (au/<? ch)))))))
