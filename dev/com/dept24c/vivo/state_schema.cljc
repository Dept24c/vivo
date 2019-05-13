(ns com.dept24c.vivo.state-schema
  (:require
   [deercreeklabs.lancaster :as l]))

(l/def-record-schema user-schema
  [:name l/string-schema]
  [:nickname l/string-schema])

(l/def-int-map-schema users-schema
  user-schema)

(l/def-record-schema msg-schema
  [:user user-schema]
  [:text l/string-schema])

(l/def-record-schema state-schema
  [:msgs (l/array-schema msg-schema)]
  [:users users-schema])
