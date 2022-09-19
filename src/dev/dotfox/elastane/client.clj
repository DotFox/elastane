(ns dev.dotfox.elastane.client)

(defprotocol Client
  (request [this req] [this req respond raise]))
