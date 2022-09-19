(ns dev.dotfox.elastane.rest
  (:require [dev.dotfox.elastane.client :as client :refer [Client]]
            [jsonista.core :as json]
            [clojure.string :as string])
  (:import (org.elasticsearch.client RestClientBuilder
                                     RestClientBuilder$HttpClientConfigCallback
                                     RestClient
                                     Request
                                     Response
                                     ResponseException
                                     ResponseListener)
           (org.apache.http HttpHost Header HttpRequest)
           (org.apache.http.message BasicHeader)
           (org.apache.http.client CredentialsProvider)
           (org.apache.http.auth AuthScope UsernamePasswordCredentials)
           (org.apache.http.impl.client BasicCredentialsProvider)
           (org.apache.http.impl.nio.client HttpAsyncClientBuilder)
           (org.apache.http.impl.nio.reactor IOReactorConfig)
           (org.apache.http.impl.nio.conn PoolingNHttpClientConnectionManager)
           (org.apache.http.impl.nio.reactor DefaultConnectingIOReactor)
           (org.apache.http.nio.entity ContentInputStream NStringEntity)
           (org.apache.http.entity ContentType)
           (java.net URL)
           (java.nio.charset StandardCharsets)
           (java.io Closeable)
           (java.util Base64)
           (java.util.function Function)
           (io.micrometer.core.instrument MeterRegistry Tag Tags)
           (io.micrometer.core.instrument.binder.httpcomponents PoolingHttpClientConnectionManagerMetricsBinder
                                                                MicrometerHttpClientInterceptor)))

(set! *warn-on-reflection* true)

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

(defn- ->tag [[^String key ^String value]]
  (Tag/of key value))

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

(defn- client-config-callback [{:keys [metrics threads auth timeout]}]
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
                    max-total RestClientBuilder/DEFAULT_MAX_CONN_TOTAL}} threads]
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
    (.addParameters query-params)))

(defn connect [urls
               & {:as opts}]
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
          (let [req (build-request req)
                ^Response res (try
                                (.performRequest rest-client req)
                                (catch ResponseException e
                                  (.getResponse e)))]
            (response->map res)))
        (request [_ req respond raise]
          (let [req (build-request req)
                listener (proxy [ResponseListener] []
                           (onSuccess [^Response res]
                             (respond (response->map res)))
                           (onFailure [^Exception exc]
                             (if (instance? ResponseException exc)
                               (let [^ResponseException response-exc exc
                                     ^Response res (.getResponse response-exc)]
                                 (respond (response->map res)))
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

(defn- body->entity [req object-mapper]
  (cond-> req
    (contains? req :body)
    (update :body (fn [obj]
                    (NStringEntity.
                     (cond
                       (map? obj)
                       (json/write-value-as-string obj object-mapper)

                       (vector? obj)
                       (string/join (map #(str (json/write-value-as-string % object-mapper) "\n") obj)))
                     ContentType/APPLICATION_JSON)))))

(defn default-middleware
  ([handler] (default-middleware handler json/keyword-keys-object-mapper))
  ([handler object-mapper]
   (fn
     ([req]
      (let [res (handler (body->entity req object-mapper))]
        (update res :body (fn [^ContentInputStream body]
                            (json/read-value body object-mapper)))))
     ([req respond raise]
      (handler (body->entity req object-mapper)
               (fn [res]
                 (respond
                  (update res :body (fn [^ContentInputStream body]
                                      (json/read-value body object-mapper)))))
               raise)))))

(comment

  (import '(io.micrometer.prometheus PrometheusMeterRegistry PrometheusConfig))

  (def pmr (PrometheusMeterRegistry. PrometheusConfig/DEFAULT))

  (def cl
    (wrap-middleware (connect "http://localhost:9207/"
                              {:auth {:type :basic-auth
                                      :params {:user "elastic"
                                               :pwd "ductile"}}
                               ;; :threads {:io-dispatch 2
                               ;;           :max-per-route 4
                               ;;           :max-total 10}
                               :metrics {:registry pmr
                                         :tags []
                                         :name "ESClient"}})
                     default-middleware))

  (.close cl)

  (time
   (dotimes [i 10000]
     (client/request cl
                     {:request-method "GET"
                      :uri "/"
                      :query-params {}}
                     (fn [_]
                       (when (= 0 (mod i 1000))
                         (println "====")
                         (println (.scrape pmr))))
                     identity)))

  )
