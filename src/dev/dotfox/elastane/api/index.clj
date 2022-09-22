(ns dev.dotfox.elastane.api.index
  (:refer-clojure :exclude [get resolve])
  (:require [dev.dotfox.elastane.api :as api]
            [dev.dotfox.elastane.client :as client]))

(defprotocol IndexAPI
  (create [this args])
  (delete [this args])
  (get [this args])
  (exists? [this args])
  (close [this args])
  (open [this args])
  (shrink [this args])
  (split [this args])
  (clone [this args])
  (rollover [this args])
  (unfreez [this args])
  (resolve [this args]))

(defn client
  ([conn]
   (client conn {}))
  ([conn default-args]
   (reify
     IndexAPI
     (create [_ args])
     (delete [_ args])
     (get [_ args])
     (exists? [_ args])
     (close [_ args])
     (open [_ args])
     (shrink [_ args])
     (split [_ args])
     (clone [_ args])
     (rollover [_ args])
     (unfreez [_ args])
     (resolve [_ args]))))

(api/gen-api-client IndexAPI
                {create {:request-method "PUT"}
                 delete {:request-method "DELETE"}})
