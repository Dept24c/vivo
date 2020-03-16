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
  [:core-user-ids (l/array-schema l/string-schema)]
  [:msgs msgs-schema]
  [:secret l/string-schema]
  [:users (l/map-schema user-schema)]
  [:user-id-to-msgs (l/map-schema msgs-schema)])

(l/def-record-schema state-schema2
  [:program-name l/string-schema]
  [:core-user-ids (l/array-schema l/string-schema)]
  [:msgs msgs-schema]
  [:secret l/string-schema]
  [:users (l/map-schema user-schema)]
  [:user-id-to-msgs (l/map-schema msgs-schema)])

(def rpcs
  {:inc {:arg-schema l/int-schema
         :ret-schema l/int-schema}
   :authed/inc {:arg-schema l/int-schema
                :ret-schema l/int-schema}})

(def rpcs2
  {:inc {:arg-schema l/long-schema
         :ret-schema l/long-schema}
   :authed/inc {:arg-schema l/long-schema
                :ret-schema l/long-schema}})
