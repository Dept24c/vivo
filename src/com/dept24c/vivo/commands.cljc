(ns com.dept24c.vivo.commands
  (:require
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]))

(defn throw-bad-path-key [path k]
  (let [disp-k (or k "nil")]
    (throw (ex-info
            (str "Illegal key `" disp-k "` in path `" path "`. Only integers, "
                 "keywords, symbols, and strings are valid path keys.")
            (u/sym-map k path)))))

(defn normalize-neg-k
  "Return the normalized key and the associated value or nil if key does not
   exist in value."
  [k v]
  (if (map? v)
    [k (v k)]
    (let [len (count v)
          norm-k (+ len k)]
      [norm-k (when (and (pos? len) (nat-int? norm-k) (< norm-k len))
                (v norm-k))])))

(defn get-in-state
  "Custom get-in fn that checks types and normalizes negative keys.
   Returns a map with :norm-path and :val keys."
  [state path]
  (reduce (fn [{:keys [norm-path val] :as acc} k]
            (let [[k* val*] (cond
                              (or (keyword? k) (nat-int? k) (string? k))
                              [k (when val
                                   (val k))]

                              (and (int? k) (neg? k))
                              (normalize-neg-k k val)

                              :else
                              (throw-bad-path-key path k))]
              (-> acc
                  (update :norm-path conj k*)
                  (assoc :val val*))))
          {:norm-path []
           :val state}
          path))

(defmulti eval-cmd (fn [state {:keys [op]}]
                     op))

(defmethod eval-cmd :set
  [state {:keys [path op arg]}]
  (let [{:keys [norm-path]} (get-in-state state path)]
    (if (seq norm-path)
      (assoc-in state norm-path arg)
      arg)))

(defmethod eval-cmd :remove
  [state {:keys [path]}]
  (let [parent-path (butlast path)
        k (last path)
        {:keys [norm-path val]} (get-in-state state parent-path)
        new-parent (if (map? val)
                     (dissoc val k)
                     (let [norm-i (if (nat-int? k)
                                    k
                                    (+ (count val) k))
                           [h t] (split-at norm-i val)]
                       (if (nat-int? norm-i)
                         (vec (concat h (rest t)))
                         val)))]
    (if (empty? norm-path)
      new-parent
      (assoc-in state norm-path new-parent))))

(defn insert* [state path op arg]
  (let [parent-path (butlast path)
        i (last path)
        _ (when-not (int? i)
            (throw (ex-info
                    (str "In " op " update expressions, the last element "
                         "of the path must be an integer, e.g. [:x :y -1] "
                         " or [:a :b :c 12]. Got: `" i "`.")
                    (u/sym-map parent-path i path op arg))))
        {:keys [norm-path val]} (get-in-state state parent-path)
        _ (when-not (or (vector? val) (nil? val))
            (throw (ex-info (str "Bad path in " op ". Path `" path "` does not "
                                 "point to a vector. Got: `" val "`.")
                            (u/sym-map op path val norm-path))))
        norm-i (if (nat-int? i)
                 i
                 (+ (count val) i))
        split-i (if (= :insert-before op)
                  norm-i
                  (inc norm-i))
        [h t] (split-at split-i val)
        new-t (cons arg t)
        new-parent (vec (concat h new-t))]
    (if (empty? norm-path)
      new-parent
      (assoc-in state norm-path new-parent))))

(defmethod eval-cmd :insert-before
  [state {:keys [path op arg]}]
  (insert* state path op arg))

(defmethod eval-cmd :insert-after
  [state {:keys [path op arg]}]
  (insert* state path op arg))

(defn eval-math-cmd [state cmd op-fn]
  (let [{:keys [path op arg]} cmd
        {:keys [norm-path val]} (get-in-state state path)
        _ (when-not (number? val)
            (throw (ex-info (str "Can't do math on non-numeric type. "
                                 "Value in state at path `"
                                 path "` is not a number. Got: " val ".")
                            (u/sym-map path cmd))))
        _ (when-not (number? arg)
            (throw (ex-info (str "Can't do math on non-numeric type. "
                                 "Arg `" arg "` in update command `"
                                 cmd "` is not a number.")
                            (u/sym-map path cmd op))))
        new-val (op-fn val arg)]
    (assoc-in state norm-path new-val)))

(defmethod eval-cmd :+
  [state cmd]
  (eval-math-cmd state cmd +))

(defmethod eval-cmd :-
  [state cmd]
  (eval-math-cmd state cmd -))

(defmethod eval-cmd :*
  [state cmd]
  (eval-math-cmd state cmd *))

(defmethod eval-cmd :/
  [state cmd]
  (eval-math-cmd state cmd /))

(defmethod eval-cmd :mod
  [state cmd]
  (eval-math-cmd state cmd mod))
