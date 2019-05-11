(ns com.dept24c.vivo.bristlecone-state-provider-impl
  (:require
   [com.dept24c.vivo.state :as state]
   [com.dept24c.vivo.utils :as u]))

(defn update-array-sub? [len sub-i update-i* op]
  (let [update-i (if (nat-int? sub-i)
                   (if (nat-int? update-i*)
                     update-i*
                     (+ len update-i*))
                   (if (nat-int? update-i*)
                     (- update-i* len)
                     update-i*))]
    (if (= :set op)
      (= sub-i update-i)
      (let [new-i (if (= :insert-after op)
                    (if (nat-int? update-i)
                      (inc update-i)
                      update-i)
                    (if (nat-int? update-i)
                      update-i
                      (dec update-i)))]
        (if (nat-int? sub-i)
          (<= new-i sub-i)
          (>= new-i sub-i))))))

(defn relationship-info
  "Given two key sequences, return a vector of [relationship tail].
   Relationsip is one of :sibling, :parent, :child, or :equal.
   Tail is the keypath between the parent and child. Tail is only defined
   when relationship is :parent."
  [ksa ksb]
  (let [va (vec ksa)
        vb (vec ksb)
        len-a (count va)
        len-b (count vb)
        len-min (min len-a len-b)
        divergence-i (loop [i 0]
                       (if (and (< i len-min)
                                (= (va i) (vb i)))
                         (recur (inc i))
                         i))
        a-tail? (> len-a divergence-i)
        b-tail? (> len-b divergence-i)]
    (cond
      (and a-tail? b-tail?) [:sibling nil]
      a-tail? [:child nil]
      b-tail? [:parent (drop divergence-i ksb)]
      :else [:equal nil])))
#_
(defn update-sub? [sub-map update-path orig-v new-v]
  ;; orig-v and new-v are guaranteed to be different
  (reduce (fn [acc subscription-path]
            (let [[relationship sub-tail] (relationship-info
                                           update-path subscription-path)]
              (case relationship
                :equal (reduced true)
                :child (reduced true)
                :sibling false
                :parent (if (= (get-in orig-v sub-tail)
                               (get-in new-v sub-tail))
                          false
                          (reduced true)))))
          false (vals sub-map)))

#_
(defn get-change-info [get-in-state update-map subs]
  ;; TODO: Handle ordered update-map with in-process vals
  (let [path->vals (reduce
                    (fn [acc [path upex]]
                      (let [orig-v (get-in-state path)
                            new-v (upex/eval orig-v upex)]
                        (if (= orig-v new-v)
                          acc
                          (assoc acc path [orig-v new-v]))))
                    {} update-map)
        subs-to-update (fn [subs path orig-v new-v]
                         (reduce (fn [acc {:keys [sub-map] :as sub}]
                                   (if (update-sub? sub-map path orig-v new-v)
                                     (conj acc sub)
                                     acc))
                                 #{} subs))]
    (reduce-kv (fn [acc path [orig-v new-v]]
                 (-> acc
                     (update :state-updates #(assoc % path new-v))
                     (update :subs-to-update set/union
                             (subs-to-update subs path orig-v new-v))))
               {:state-updates {}
                :subs-to-update #{}}
               path->vals)))
#_
(defn update-state* [update-map get-in-state set-paths-in-state! subs]
  (let [{:vivo/keys [tx-info-str]} update-map
        update-map* (dissoc update-map :vivo/tx-info-str)
        {:keys [state-updates subs-to-update]} (get-change-info
                                                get-in-state update-map* subs)]
    (set-paths-in-state! state-updates)
    (doseq [{:keys [update-fn sub-map]} subs-to-update]
      (let [data-frame (cond-> (make-data-frame get-in-state sub-map)
                         tx-info-str (assoc :vivo/tx-info-str tx-info-str))]
        (update-fn data-frame)))
    true))
