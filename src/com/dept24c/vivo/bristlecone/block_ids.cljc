(ns com.dept24c.vivo.bristlecone.block-ids
  (:require
   [clojure.string :as string]
   #?(:cljs [goog.math :as gm])))

#?(:cljs (def class type))
#?(:cljs (def Long gm/Long))

(def max-int 2147483647)
(def min-int -2147483648)
(def one #?(:clj 1 :cljs (gm/Long.getOne)))
(def sixty-two #?(:clj 62 :cljs (gm/Long.fromInt 62)))
(def zero #?(:clj 0 :cljs  (gm/Long.fromInt 0)))

(defn long? [x]
  (if x
    (boolean (= Long (class x)))
    false))

(defn long= [a b]
  #?(:clj (= a b)
     :cljs (cond
             (long? a) (.equals ^Long a b)
             (long? b) (.equals ^Long b a)
             :else (= a b))))

#?(:cljs (extend-type Long
           IEquiv
           (-equiv [l other]
             (long= l other))))

#?(:cljs (extend-type Long
           IHash
           (-hash [l]
             (bit-xor (.getLowBits ^Long l) (.getHighBits ^Long l)))))

#?(:cljs (extend-type Long
           IComparable
           (-compare [l other]
             (.compare ^Long l other))))

(defn ints->long [high low]
  #?(:clj (bit-or (bit-shift-left (long high) 32)
                  (bit-and low 0xFFFFFFFF))
     :cljs (.fromBits ^Long Long (int low) (int high))))

(defn long->ints [l]
  (let [high (int #?(:clj (bit-shift-right l 32)
                     :cljs (.getHighBits ^Long l)))
        low (int #?(:clj (.intValue ^Long l)
                    :cljs (.getLowBits ^Long l)))]
    [high low]))

(defn- throw-long->int-err [l]
  (throw (ex-info (str "Cannot convert long `" l "` to int.")
                  {:input l
                   :class-of-input (class l)})))

(defn long->int [l]
  (if-not (long? l)
    l
    #?(:clj (if (and (<= ^Long l max-int) (>= ^Long l min-int))
              (.intValue ^Long l)
              (throw-long->int-err l))
       :cljs (if (and (.lessThanOrEqual ^Long l (gm/Long.fromInt max-int))
                      (.greaterThanOrEqual ^Long l (gm/Long.fromInt min-int)))
               (.toInt ^Long l)
               (throw-long->int-err ^Long l)))))

(defn ensure-long [n]
  (if (long? n)
    n
    (if-not (number? n)
      (throw (ex-info (str "n is not a number. Got: `" (or n "nil") "`.")
                      {:n n}))
      (if (<= n max-int)
        (ints->long 0 n)
        (throw (ex-info "Number is too large to be converted accurately to Long."
                        {:n n}))))))

(defn str->long [s]
  #?(:clj (Long/parseLong s)
     :cljs (gm/Long.fromString s)))

(defn long->str [l]
  #?(:clj (str l)
     :cljs (.toString ^Long l)))

(defn long-mod [a b]
  #?(:clj (mod a b)
     :cljs (.modulo ^Long a ^Long b)))

(defn long-quot [a b]
  #?(:clj (quot a b)
     :cljs (.div ^Long a ^Long b)))

(defn long-zero? [l]
  #?(:clj (zero? l)
     :cljs (.isZero ^Long l)))

(defn long-neg? [l]
  #?(:clj (neg? l)
     :cljs (if (long? l)
             (.isNegative ^Long l)
             (neg? l))))

(defn long-pos? [l]
  #?(:clj (pos? l)
     :cljs (if (long? l)
             (.greaterThan ^Long l zero)
             (pos? l))))

(defn long-inc [l]
  #?(:clj (inc l)
     :cljs (if (long? l)
             (.add ^Long l one)
             (inc l))))

(defn long-dec [l]
  #?(:clj (dec l)
     :cljs (if (long? l)
             (.subtract ^Long l one)
             (dec l))))

(defn long-add [a b]
  #?(:clj (+ a b)
     :cljs (.add ^Long (ensure-long a) ^Long (ensure-long b))))

(defn long-mul [a b]
  #?(:clj (* a b)
     :cljs (.multiply ^Long (ensure-long a) ^Long (ensure-long b))))

(defn long-lt [a b]
  #?(:clj (< a b)
     :cljs (.lessThan ^Long (ensure-long a) ^Long (ensure-long b))))

(defn long-gt [a b]
  #?(:clj (> a b)
     :cljs (.greaterThan ^Long (ensure-long a) ^Long (ensure-long b))))

;; Encoding for block-ids
;; - Designed to work w/ allowed DDB name characters while leaving
;;   non-alphanumeric characters for other uses.
;; - Left-most character changes fastest, spreading block ids across
;;   DDB partitions.

(def b62alphabet
  [\A \B \C \D \E \F \G \H \I \J \K \L \M \N \O \P \Q \R \S \T \U \V \W \X \Y \Z
   \a \b \c \d \e \f \g \h \i \j \k \l \m \n \o \p \q \r \s \t \u \v \w \x \y \z
   \0 \1 \2 \3 \4 \5 \6 \7 \8 \9])

(def b62char->index
  (reduce (fn [acc [i c]]
            (assoc acc c i))
          {} (map-indexed vector b62alphabet)))

(def b62pos->weight
  (mapv str->long
        ["1" "62" "3844" "238328" "14776336" "916132832" "56800235584"
         "3521614606208" "218340105584896" "13537086546263552"
         "839299365868340224"]))

(defn ulong->b62 [l]
  (let [ln (ensure-long l)]
    (when (long-neg? ln)
      (throw (ex-info "ulong->b62 is only defined on non-negative integers."
                      {:given-arg l})))
    (loop [i 0
           n ln
           s ""]
      (let [c (b62alphabet (long->int (long-mod n sixty-two)))
            new-i (inc i)
            new-n (long-quot n sixty-two)
            new-s (str s c)]
        (if (long-zero? new-n)
          new-s
          (recur new-i new-n new-s))))))

(defn b62->ulong [^String s]
  (when-not (string? s)
    (throw (ex-info "Argument to b62->ulong must be a string."
                    {:given-arg s})))
  (let [last-i (dec (count s))]
    (loop [i 0
           l (ensure-long 0)]
      (let [c (.charAt s i)
            v (long-mul (b62char->index c) (b62pos->weight i))
            new-l (long-add l v)]
        (if (= last-i i)
          new-l
          (recur (inc i) new-l))))))

(defn temp-block-id? [block-id]
  (when-not (string? block-id)
    (throw (ex-info (str "Bad block-id arg to temp-block-id?. block-id must be "
                         "a string. Got `" (or block-id "nil") "`.")
                    {:block-id block-id})))
  (string/starts-with? block-id "-"))

(defn block-id->temp-block-id [block-id]
  (when block-id
    (if (temp-block-id? block-id)
      block-id
      (str "-" block-id))))

(defn block-num->block-id [block-num]
  (if (long-neg? block-num)
    (block-id->temp-block-id (ulong->b62 (* -1 block-num)))
    (ulong->b62 block-num)))

(defn block-id->block-num [block-id]
  (if (temp-block-id? block-id)
    (* -1 (b62->ulong (subs block-id 1)))
    (b62->ulong block-id)))

(defn earlier? [block-id1 block-id2]
  (let [block-num1 (block-id->block-num block-id1)
        block-num2 (block-id->block-num block-id2)
        block-type #(if (temp-block-id? %) "temporary" "permanent")]
    (cond
      (and (long-pos? block-num1) (long-pos? block-num2))
      (long-lt block-num1 block-num2)

      (and (long-neg? block-num1) (long-neg? block-num2))
      (long-gt block-num1 block-num2)

      :else
      (throw (ex-info
              (str "Mismatched block-ids. block-id1 is " (block-type block-id1)
                   " but block-id2 is " (block-type block-id2) ".")
              {:block-id1 block-id1
               :block-id2 block-id2})))))
