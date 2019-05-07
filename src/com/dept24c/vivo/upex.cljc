(ns com.dept24c.vivo.upex
  (:refer-clojure :exclude [eval])
  (:require
   [com.dept24c.vivo.utils :as u]))

(defmulti eval (fn [orig-v upex]
                 (first upex)))

;; TODO: State arg count error msgs more clearly.

(defmethod eval :set
  [orig-v upex]
  (when-not (= 2 (count upex))
    (throw (ex-info (str ":set update expressions must have exactly one "
                         " argument. Got " (count upex) ".")
                    (u/sym-map orig-v upex))))
  (let [[op v] upex]
    v))

(defmethod eval :assoc
  [orig-v upex]
  (when-not (or (map? orig-v) (nil? orig-v))
    (throw (ex-info (str ":assoc may only be used on a map/record value. Got: "
                         orig-v)
                    (u/sym-map orig-v upex))))
  (let [[op & kvs] upex]
    (apply assoc orig-v kvs)))

(defmethod eval :dissoc
  [orig-v upex]
  (when-not (map? orig-v)
    (throw (ex-info (str ":dissoc may only be used on a map/record value. Got: "
                         orig-v)
                    (u/sym-map orig-v upex))))
  (let [[op & ks] upex]
    (apply dissoc orig-v ks)))

(defmethod eval :append
  [orig-v upex]
  (when-not (or (sequential? orig-v) (nil? orig-v))
    (throw (ex-info (str ":append may only be used on a seqential value. Got: "
                         orig-v)
                    (u/sym-map orig-v upex))))
  (when-not (= 2 (count upex))
    (throw (ex-info (str "Wrong argument count (" (count upex) ") for :append. "
                         "Expected 2 arguments.")
                    (u/sym-map orig-v upex))))
  (let [[op v] upex]
    (conj (vec orig-v) v)))

(defmethod eval :prepend
  [orig-v upex]
  (when-not (or (sequential? orig-v) (nil? orig-v))
    (throw (ex-info (str ":prepend may only be used on a seqential value. Got: "
                         orig-v)
                    (u/sym-map orig-v upex))))
  (when-not (= 2 (count upex))
    (throw (ex-info (str "Wrong argument count (" (count upex) ") for "
                         ":prepend. Expected 2 arguments.")
                    (u/sym-map orig-v upex))))
  (let [[op v] upex]
    (vec (cons v orig-v))))

(defmethod eval :+
  [orig-v upex]
  (when-not (number? orig-v)
    (throw (ex-info (str ":+ may only be used on a numeric value. Got: " orig-v)
                    (u/sym-map orig-v upex))))
  (when-not (= 2 (count upex))
    (throw (ex-info (str "Wrong argument count (" (count upex) ") for :+. "
                         "Expected 2 parameter. e.g. [:+ 10")
                    (u/sym-map orig-v upex))))
  (let [[op v] upex]
    (+ orig-v v)))

;; TODO: Add drop, drop-last, concat fns, + -
