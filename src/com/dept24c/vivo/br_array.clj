(ns com.dept24c.vivo.br-array
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [com.dept24c.vivo.bristlecone :as br]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as lu]))

(defmulti handle-cmd (fn [{:keys [op]} & _]
                       op))

(def small-types #{:int :long :float :double :nil :boolean :enum})

(defn child-edn-schema [edn-schema]
  (let [item-schema (:items edn-schema)
        item-type (lu/get-avro-type item-schema)]
    (cond
      (small-types item-type)
      item-schema

      (and (= :fixed item-type)
           (<= 10 (:size item-schema)))
      item-schema

      :else
      :block)))

(defmethod br/<update* :array
  [pschema cmd db-id storage]
  (let [edn (l/edn pschema)
        child-edn-schema (child-edn-schema edn)]
    (handle-cmd cmd db-id storage)))

(defmethod handle-cmd :insert-after
  [cmd db-id storage]
  (let [{:keys [path arg]} cmd]
    (println "#####")
    (u/pprint (u/sym-map path arg db-id))
    :blah))
