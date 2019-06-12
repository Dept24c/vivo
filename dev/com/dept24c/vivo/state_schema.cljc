(ns com.dept24c.vivo.state-schema
  (:require
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.bilt :as bilt]))

(l/def-record-schema user-schema
  [:name l/string-schema]
  [:nickname l/string-schema])

(def users-schema (bilt/int-map-schema user-schema))

(l/def-record-schema msg-schema
  [:user user-schema]
  [:text l/string-schema])

(l/def-record-schema state-schema
  [:app-name l/string-schema]
  [:msgs (l/array-schema msg-schema)]
  [:secret l/string-schema]
  [:users users-schema])
