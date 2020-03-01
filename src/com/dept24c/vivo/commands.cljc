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
  ([state path]
   (get-in-state state path nil))
  ([state path prefixes*]
   (let [prefixes (cond
                    (nil? prefixes*) #{}
                    (set? prefixes*) prefixes*
                    :else (set [prefixes*]))
         [path-head & path-tail] path]
     (when (and (seq prefixes)
                (not (prefixes path-head)))
       (throw (ex-info (str "Illegal path. Path must start with one of "
                            prefixes ". Got `" path "`.")
                       (u/sym-map path prefixes path-head))))
     (reduce (fn [{:keys [val] :as acc} k]
               (let [[k* val*] (cond
                                 (or (keyword? k) (nat-int? k) (string? k))
                                 [k (when val
                                      (get val k))]

                                 (and (int? k) (neg? k))
                                 (normalize-neg-k k val)

                                 (nil? k)
                                 [nil nil]

                                 :else
                                 (throw-bad-path-key path k))]
                 (-> acc
                     (update :norm-path conj k*)
                     (assoc :val val*))))
             {:norm-path (if (seq prefixes)
                           [path-head]
                           [])
              :val state}
             (if (seq prefixes)
               path-tail
               path)))))

(defmulti eval-cmd (fn [state {:keys [op]} prefix]
                     op))

(defmethod eval-cmd :set
  [state {:keys [path op arg]} prefix]
  (let [{:keys [norm-path]} (get-in-state state path prefix)
        state-path (if prefix
                     (rest norm-path)
                     norm-path)
        new-state (if (seq state-path)
                    (assoc-in state state-path arg)
                    arg)]
    {:state new-state
     :update-info {:norm-path norm-path
                   :op op
                   :value arg}}))

(defmethod eval-cmd :remove
  [state {:keys [path op]} prefix]
  (let [parent-path (butlast path)
        k (last path)
        {:keys [norm-path val]} (get-in-state state parent-path prefix)
        [new-parent path-k] (if (map? val)
                              [(dissoc val k) k]
                              (let [norm-i (if (nat-int? k)
                                             k
                                             (+ (count val) k))
                                    [h t] (split-at norm-i val)]
                                (if (nat-int? norm-i)
                                  [(vec (concat h (rest t))) norm-i]
                                  (throw (ex-info "Path index out of range."
                                                  (u/sym-map norm-i path
                                                             norm-path))))))
        state-path (if prefix
                     (rest norm-path)
                     norm-path)
        new-state (if (empty? state-path)
                    new-parent
                    (assoc-in state state-path new-parent))]
    {:state new-state
     :update-info {:norm-path (conj norm-path path-k)
                   :op op
                   :value nil}}))

(defn insert* [state path prefix op arg]
  (let [parent-path (butlast path)
        i (last path)
        _ (when-not (int? i)
            (throw (ex-info
                    (str "In " op " update expressions, the last element "
                         "of the path must be an integer, e.g. [:x :y -1] "
                         " or [:a :b :c 12]. Got: `" i "`.")
                    (u/sym-map parent-path i path op arg))))
        {:keys [norm-path val]} (get-in-state state parent-path prefix)
        _ (when-not (or (sequential? val) (nil? val))
            (throw (ex-info (str "Bad path in " op ". Path `" path "` does not "
                                 "point to a sequence. Got: `" val "`.")
                            (u/sym-map op path val norm-path))))
        norm-i (if (nat-int? i)
                 i
                 (+ (count val) i))
        split-i (if (= :insert-before op)
                  norm-i
                  (inc norm-i))
        [h t] (split-at split-i val)
        new-t (cons arg t)
        new-parent (vec (concat h new-t))
        state-path (if prefix
                     (rest norm-path)
                     norm-path)
        new-state (if (empty? state-path)
                    new-parent
                    (assoc-in state state-path new-parent))]
    {:state new-state
     :update-info {:norm-path (conj norm-path split-i)
                   :op op
                   :value arg}}))

(defmethod eval-cmd :insert-before
  [state {:keys [path op arg]} prefix]
  (insert* state path prefix op arg))

(defmethod eval-cmd :insert-after
  [state {:keys [path op arg]} prefix]
  (insert* state path prefix op arg))

(defn eval-math-cmd [state cmd prefix op-fn]
  (let [{:keys [path op arg]} cmd
        {:keys [norm-path val]} (get-in-state state path prefix)
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
        new-val (op-fn val arg)
        state-path (if prefix
                     (rest norm-path)
                     norm-path)]
    {:state (assoc-in state state-path new-val)
     :update-info {:norm-path norm-path
                   :op op
                   :value new-val}}))

(defmethod eval-cmd :+
  [state cmd prefix]
  (eval-math-cmd state cmd prefix +))

(defmethod eval-cmd :-
  [state cmd prefix]
  (eval-math-cmd state cmd prefix -))

(defmethod eval-cmd :*
  [state cmd prefix]
  (eval-math-cmd state cmd prefix *))

(defmethod eval-cmd :/
  [state cmd prefix]
  (eval-math-cmd state cmd prefix /))

(defmethod eval-cmd :mod
  [state cmd prefix]
  (eval-math-cmd state cmd prefix mod))
