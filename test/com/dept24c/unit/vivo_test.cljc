(ns com.dept24c.unit.vivo-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [are deftest is]]
   [com.dept24c.vivo :as vivo]
   [com.dept24c.vivo.macro-impl :as macro-impl]
   [com.dept24c.vivo.state-manager-impl :as smi]
   [com.dept24c.vivo.state-provider :as sp]
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

(deftest test-relationship
  (are [ret ks1 ks2] (= ret (sp/relationship-info ks1 ks2))
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

(vivo/def-subscriber a-subscriber
  {name [:local :name]}
  [sm ch]
  (ca/put! ch name))

(deftest test-def-subscriber
  (au/test-async
   3000
   (ca/go
     (let [sm (vivo/state-manager {:local (vivo/mem-state-provider)})
           ch (ca/chan 1)
           sub (a-subscriber sm ch)
           name "Alice"]
       (vivo/update-state! sm {[:local] [:assoc :name name]})
       (is (= name (au/<? ch)))))))

(deftest test-edn<->str
  (are [edn] (= edn (u/str->edn (u/edn->str edn)))
    "string"
    {:a 1 :b 2}
    [7 :v :x "str"]
    {"s" 1 :s 2}))

(deftest test-bad-path-root
  (let [sm (vivo/state-manager
            {:local (vivo/mem-state-provider {})})]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Invalid path root"
         (vivo/update-state! sm {[:not-a-valid-root :x] [:assoc :y 1]})))))

;; TODO: Test :vivo/tx-info
;; TODO: Test a series of update-state! calls
;; TODO: Test dissoc'ing a key from the tree
