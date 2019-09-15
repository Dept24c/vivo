(ns com.dept24c.vivo.state-schema
  (:require
   [deercreeklabs.lancaster :as l]))

(l/def-record-schema user-schema
  [:name l/string-schema]
  [:nickname l/string-schema])

(l/def-record-schema msg-schema
  [:user-id l/string-schema]
  [:text l/string-schema])

(l/def-array-schema msgs-schema msg-schema)

(l/def-record-schema state-schema
  [:app-name l/string-schema]
  [:msgs msgs-schema]
  [:secret l/string-schema]
  [:users (l/map-schema user-schema)]
  [:user-id-to-msgs (l/map-schema msgs-schema)])
