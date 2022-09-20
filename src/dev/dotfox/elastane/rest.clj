(ns dev.dotfox.elastane.rest
  (:require [clojure.string :as string]
            [dev.dotfox.elastane.client :as client :refer [Client]]
            [jsonista.core :as json])
  (:import (io.micrometer.core.instrument MeterRegistry
                                          Tag
                                          Tags)
           (io.micrometer.core.instrument.binder.httpcomponents PoolingHttpClientConnectionManagerMetricsBinder
                                                                MicrometerHttpClientInterceptor)
           (java.io ByteArrayOutputStream Closeable)
           (java.net URL)
           (java.nio.charset StandardCharsets)
           (java.util Base64)
           (java.util.function Function)
           (org.apache.http HttpHost
                            Header
                            HttpRequest)
           (org.apache.http.auth AuthScope
                                 UsernamePasswordCredentials)
           (org.apache.http.client CredentialsProvider)
           (org.apache.http.entity ContentType)
           (org.apache.http.impl.client BasicCredentialsProvider)
           (org.apache.http.impl.nio.client HttpAsyncClientBuilder)
           (org.apache.http.impl.nio.conn PoolingNHttpClientConnectionManager)
           (org.apache.http.impl.nio.reactor IOReactorConfig)
           (org.apache.http.impl.nio.reactor DefaultConnectingIOReactor)
           (org.apache.http.message BasicHeader)
           (org.apache.http.nio.entity ContentInputStream
                                       NByteArrayEntity)
           (org.elasticsearch.client RestClientBuilder
                                     RestClientBuilder$HttpClientConfigCallback
                                     RestClient
                                     Request
                                     Response
                                     ResponseException
                                     ResponseListener)))

(set! *warn-on-reflection* true)

(def ^:private ^ContentType json-content-type ContentType/APPLICATION_JSON)
(def ^:private ^ContentType ndjson-content-type (ContentType/create "application/x-ndjson" StandardCharsets/UTF_8))

(defn- get-port [^URL url]
  (let [p (.getPort url)]
    (when-not (= -1 p)
      p)))

(defn- ->http-host [^String url]
  (let [url (URL. url)
        ^String protocol (.getProtocol url)
        port (int (or (get-port url)
                      (and (= protocol "http") 80)
                      (and (= protocol "https") 443)))
        ^String hostname (.getHost url)]
    (HttpHost. hostname
               port
               protocol)))

(defn- ->tag [[key ^String value]]
  (Tag/of (name key) value))

(defn- ->tags ^Tags [tags]
  (let [^"[Lio.micrometer.core.instrument.Tag;" tags (into-array Tag (map ->tag tags))]
    (Tags/of tags)))

(defn- header->map-entry [^Header header]
  [(.getName header) (.getValue header)])

(defn- headers->map [^"[Lorg.apache.http.Header;" headers]
  (into {}
        (map header->map-entry)
        headers))

(defn- auth-header [value]
  (when value {"Authorization" value}))

(defn- bearer-header [{:keys [token]}]
  (auth-header (if (string/starts-with? token "Bearer ")
                 token
                 (str "Bearer " token))))

(defn- oauth-header [{:keys [token]}]
  (auth-header token))

(defn- api-key-header [{:keys [id api-key]}]
  (auth-header (str "ApiKey " (.encodeToString (Base64/getEncoder)
                                               (.getBytes (format "%s:%s" id api-key)
                                                          StandardCharsets/UTF_8)))))

(defn- default-headers [{:keys [auth headers]}]
  (let [{:keys [type params]} auth
        auth-header (case type
                      :bearer (bearer-header params)
                      :oauth-token (oauth-header params)
                      :api-key (api-key-header params)
                      nil)]
    (into-array Header (map-indexed (fn ^BasicHeader [header value]
                                      (BasicHeader. header value))
                                    (merge headers auth-header)))))

(defn- client-config-callback [{:keys [metrics threads connections auth timeout]}]
  (let [^CredentialsProvider credentials-provider
        (when (= :basic-auth (:type auth))
          (let [{{:keys [^String user
                         ^String pwd]} :params} auth
                user-pwd-credentials (UsernamePasswordCredentials. user pwd)]
            (doto (BasicCredentialsProvider.)
              (.setCredentials AuthScope/ANY user-pwd-credentials))))
        ^IOReactorConfig io-reactor-config
        (let [{:keys [io-dispatch]
               :or {io-dispatch (.availableProcessors (Runtime/getRuntime))}} threads
              {:keys [socket connect]
               :or {socket 0
                    connect 0}} timeout]
          (-> (IOReactorConfig/copy IOReactorConfig/DEFAULT)
              (.setIoThreadCount io-dispatch)
              (.setSoKeepAlive true)
              (.setSoTimeout socket)
              (.setConnectTimeout connect)
              (.build)))
        ^PoolingNHttpClientConnectionManager conn-manager
        (let [{:keys [max-per-route max-total]
               :or {max-per-route RestClientBuilder/DEFAULT_MAX_CONN_PER_ROUTE
                    max-total RestClientBuilder/DEFAULT_MAX_CONN_TOTAL}} connections]
          (doto (PoolingNHttpClientConnectionManager.
                 (DefaultConnectingIOReactor. io-reactor-config))
            (.setDefaultMaxPerRoute max-per-route)
            (.setMaxTotal max-total)))
        set-interceptors
        (if-let [{:keys [^MeterRegistry registry tags ^String name]} metrics]
          (let [^Tags tags (->tags tags)
                interceptor (MicrometerHttpClientInterceptor.
                             registry
                             (reify Function
                               (apply [_ request]
                                 (let [^HttpRequest request request]
                                   (-> request
                                       (.getRequestLine)
                                       (.getUri)))))
                             tags
                             true)]
            (.bindTo (PoolingHttpClientConnectionManagerMetricsBinder.
                      conn-manager
                      name
                      tags)
                     registry)
            (fn [^HttpAsyncClientBuilder builder]
              (-> builder
                  (.addInterceptorFirst (.getRequestInterceptor interceptor))
                  (.addInterceptorLast (.getResponseInterceptor interceptor)))))
          identity)]
    (proxy [RestClientBuilder$HttpClientConfigCallback] []
      (customizeHttpClient [^HttpAsyncClientBuilder builder]
        (-> builder
            (.setDefaultIOReactorConfig io-reactor-config)
            (.setConnectionManager conn-manager)
            (.setDefaultCredentialsProvider credentials-provider)
            (set-interceptors))))))

(defn- response->map [^Response res]
  {:status (-> res (.getStatusLine) (.getStatusCode))
   :headers (-> res (.getHeaders) (headers->map))
   :body (-> res (.getEntity) (.getContent))})

(defn- build-request ^Request [{:keys [request-method uri query-params body]}]
  (doto (Request. (string/upper-case (name request-method)) uri)
    (.setEntity body)
    (.addParameters (reduce-kv
                     (fn [acc k v]
                       (assoc acc (name k) v))
                     {}
                     query-params))))

(defn- body->entity [req object-mapper]
  (cond-> req
    (contains? req :body)
    (update :body (fn [obj]
                    (let [baos (ByteArrayOutputStream.)]
                      (cond
                        (map? obj)
                        (json/write-value baos obj object-mapper)

                        (vector? obj)
                        (doseq [v obj]
                          (json/write-value baos v object-mapper)
                          (.write baos (.getBytes "\n" StandardCharsets/UTF_8))))
                      (NByteArrayEntity. (.toByteArray baos) (if (map? obj) json-content-type ndjson-content-type)))))))

(defn- entity->body [res object-mapper]
  (cond-> res
    (contains? res :body)
    (update :body (fn [^ContentInputStream body]
                    (json/read-value body object-mapper)))))

(defn connect [urls
               & {:keys [object-mapper]
                  :or {object-mapper json/keyword-keys-object-mapper}
                  :as opts}]
  (try
    (let [urls (if (string? urls) [urls] urls)
          ^"[Lorg.apache.http.HttpHost;" http-hosts (into-array HttpHost (map ->http-host urls))
          ^RestClientBuilder$HttpClientConfigCallback client-config-callback (client-config-callback opts)
          ^"[Lorg.apache.http.Header;" default-headers (default-headers opts)
          ^RestClientBuilder builder (-> (RestClient/builder http-hosts)
                                         (.setCompressionEnabled true)
                                         (.setHttpClientConfigCallback client-config-callback)
                                         (.setDefaultHeaders default-headers))
          ^RestClient rest-client (.build builder)]
      (reify
        Client
        (request [_ req]
          (let [req (-> req (body->entity object-mapper) build-request)
                ^Response res (try
                                (.performRequest rest-client req)
                                (catch ResponseException e
                                  (.getResponse e)))]
            (-> res
                (response->map)
                (entity->body object-mapper))))
        (request [_ req respond raise]
          (let [req (-> req (body->entity object-mapper) build-request)
                listener (proxy [ResponseListener] []
                           (onSuccess [^Response res]
                             (respond (-> res
                                          (response->map)
                                          (entity->body object-mapper))))
                           (onFailure [^Exception exc]
                             (if (instance? ResponseException exc)
                               (let [^ResponseException response-exc exc
                                     ^Response res (.getResponse response-exc)]
                                 (respond (-> res
                                              (response->map)
                                              (entity->body object-mapper))))
                               (raise exc))))]
            (.performRequestAsync rest-client req listener)))

        Closeable
        (close [_]
          (.close rest-client))))
    (catch Exception e
      (throw (ex-info "Unable to connect to Elasticsearch cluster"
                      {:urls urls}
                      e)))))

(defn wrap-middleware [^Closeable client middleware]
  (reify
    Client
    (request [_ req]
      ((middleware #(client/request client %)) req))
    (request [_ req respond raise]
      ((middleware #(client/request client %1 %2 %3)) req respond raise))

    Closeable
    (close [_] (.close client))))

(comment

  (import '(io.micrometer.prometheus PrometheusMeterRegistry PrometheusConfig))

  (def pmr (PrometheusMeterRegistry. PrometheusConfig/DEFAULT))

  (def conn
    (connect "http://localhost:9207/"
             {:auth {:type :basic-auth
                     :params {:user "elastic"
                              :pwd "ductile"}}
              :connections {:max-per-route 100
                            :max-total 100}
              :metrics {:registry pmr
                        :tags {:app "SAMPLE"}
                        :name "ESClient"}}))

  (.close conn)

  (client/request conn {:request-method "PUT"
                        :uri "/new-index"
                        :body {:settings {:index {:number_of_shards 3
                                                  :number_of_replicas 2}}}})

  (client/request conn {:request-method "GET"
                        :uri "/new-index/_msearch"
                        :body [{}
                               {:query {:match {:message "this is a test"}}}
                               {:index "new-index-2"}
                               {:query {:match_all {}}}]})

  (time
   (dotimes [i 10000]
     (client/request conn
                     {:request-method "GET"
                      :uri "/_cluster/health"}
                     (fn [_]
                       (when (= 0 (mod i 1000))
                         (println "====")
                         (println (.scrape pmr))))
                     identity)))

  (println (.scrape pmr))

  )
