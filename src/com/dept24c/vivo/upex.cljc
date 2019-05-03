(ns com.dept24c.vivo.upex
  (:require
   [com.dept24c.vivo.utils :as u]))

(defmulti execute (fn [orig-v upex]
                    (first upex)))

;; TODO: State arg count error msgs more clearly.

(defmethod execute :assoc
  [orig-v upex]
  (when-not (or (map? orig-v) (nil? orig-v))
    (throw (ex-info (str ":assoc may only be used on a map/record value. Got: "
                         orig-v)
                    (u/sym-map orig-v upex))))
  (let [[op & kvs] upex]
    (apply assoc orig-v kvs)))

(defmethod execute :dissoc
  [orig-v upex]
  (when-not (map? orig-v)
    (throw (ex-info (str ":dissoc may only be used on a map/record value. Got: "
                         orig-v)
                    (u/sym-map orig-v upex))))
  (let [[op & ks] upex]
    (apply dissoc orig-v ks)))

(defmethod execute :append
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

(defmethod execute :prepend
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

(defmethod execute :+
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
