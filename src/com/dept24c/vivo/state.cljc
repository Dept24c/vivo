(ns com.dept24c.vivo.state)

(defprotocol IState
  (update-state! [this update-map tx-info cb])
  (subscribe! [this sub-id sub-map update-fn])
  (unsubscribe! [this sub-id]))
