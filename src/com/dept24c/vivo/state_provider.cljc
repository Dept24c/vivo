(ns com.dept24c.vivo.state-provider
  (:require
   [clojure.set :as set]
   [com.dept24c.vivo.state :as state]
   [com.dept24c.vivo.upex :as upex]
   [com.dept24c.vivo.utils :as u]
   ;;[deercreeklabs.capsule.client :as cc]
   ))

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

(defn make-data-frame [get-in-state sub-map]
  (reduce-kv (fn [acc df-key path]
               (assoc acc df-key (get-in-state path)))
             {} sub-map))

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

(defn subscribe* [sub-map update-fn get-in-state]
  (let [sub (u/sym-map sub-map update-fn)
        data-frame (make-data-frame get-in-state sub-map)]
    (update-fn data-frame)
    sub))

(defrecord MemStateProvider [*sub-id->sub *state]
  state/IState
  (update-state! [this update-map]
    ;; TODO: Rework to not pass entire state to [:assoc...] when path is []
    (let [set-paths-in-state! (fn [state-updates]
                                (swap! *state
                                       (fn [state]
                                         (reduce-kv
                                          (fn [acc path v]
                                            (if (seq path)
                                              (assoc-in acc path v)
                                              (merge acc v)))
                                          state state-updates))))]
      (update-state* update-map #(get-in @*state %) set-paths-in-state!
                     (vals @*sub-id->sub))))

  (subscribe! [this sub-id sub-map update-fn]
    (let [state @*state
          get-in-state #(get-in state %)
          sub (subscribe* sub-map update-fn get-in-state)]
      (swap! *sub-id->sub assoc sub-id sub)))

  (unsubscribe! [this sub-id]
    (swap! *sub-id->sub dissoc sub-id)))

(defn mem-state-provider [initial-state]
  (let [*sub-id->sub (atom {})
        *state (atom initial-state)]
    (->MemStateProvider *sub-id->sub *state)))
