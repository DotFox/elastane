(ns dev.dotfox.elastane.api
  (:require [clojure.string :as string]))

(def common-params-mapping
  {:index :path-params
   :wait_for_active_shards :query-params
   :master_timeout :query-params
   :timeout :query-params
   :aliases :body
   :mappings :body
   :settings :body})

(defn params->request [request-method uri-template params]
  (let [{:keys [path-params
                query-params
                body]}
        (reduce-kv
         (fn [acc k v]
           (update acc (common-params-mapping k)
                   assoc k v))
         {:request-method request-method}
         params)]
    (merge {:uri (uri-template path-params)
            :request-method request-method}
           (when query-params
             {:query-params query-params})
           (when body
             {:body body}))))

(defn make-uri-template [st]
  (fn [path-params]
    ))

(defmacro gen-api-client [api-name methods]
  `(do
     (defprotocol ~api-name
       ~@(map (fn [method-name]
                `(~method-name [~'this ~'args]))
           (keys methods)))
     (defrecord ~(symbol (str (name api-name) "Client")) [~'conn ~'default-args]
         ~api-name
       ~@(map (fn [[method-name spec]]
                `(~method-name [_this# args#]
                  ~spec))
           methods))))

(gen-api-client IndexAPI
                {create {:request-method "PUT"}
                 delete {:request-method "DELETE"}})

(comment

  (require '[jsonista.core :as json]
           '[clojure.java.io :as io])

  (def schema (json/read-value (io/file "/Users/kchernys/Projects/cisco/elasticsearch-specification/output/schema/schema.json")))

  (keys schema)

  (sequence
   (comp (map #(get % "name"))
         (map #(string/split % #"\."))
         (filter #(= (first %) "_internal")))
   (get schema "endpoints"))

  )
