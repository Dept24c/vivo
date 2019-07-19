(ns com.dept24c.vivo.state-schema
  (:require
   [deercreeklabs.lancaster :as l]))

(l/def-record-schema user-schema
  [:name l/string-schema]
  [:nickname l/string-schema])

(l/def-int-map-schema users-schema
  user-schema)

(l/def-record-schema msg-schema
  [:user-id l/int-schema]
  [:text l/string-schema])

(l/def-array-schema msgs-schema msg-schema)

(l/def-int-map-schema user-id-to-msgs-schema
  msgs-schema)

(l/def-record-schema state-schema
  [:app-name l/string-schema]
  [:msgs msgs-schema]
  [:secret (l/maybe l/string-schema)]
  [:users users-schema]
  [:user-id-to-msgs (l/maybe user-id-to-msgs-schema)])
