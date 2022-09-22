(ns dev.dotfox.elastane.json-schema
  (:require [jsonista.core :as json]
            [clojure.java.io :as io]
            [clojure.walk :as walk]
            [clojure.string :as string]
            [malli.core :as m]
            [malli.util :as mu]
            [malli.dot :as md]
            [malli.error :as me]
            [dev.dotfox.elastane.client :as client]
            [dev.dotfox.elastane.rest :as rest]))

(defmulti -convert-type (fn [{:keys [kind]} _] kind))

(def convert-type (memoize -convert-type))

(defn- find-type [[t & map-entries] key]
  (if (= :map t)
    (first (filter #(= key (first %)) map-entries))
    (throw (ex-info "" {}))))

(defn- add-props [schema props]
  (let [[t & args] schema
        base-props (if (map? (first args))
                     (first args)
                     {})
        args (if (map? (first args))
               (rest args)
               args)]
    (into [t (merge base-props props)] args)))

(defn- make-shortcut [schema prop]
  (let [prop-schema (find-type schema (keyword prop))]
    [:or (last prop-schema) schema]))

(defn- merge-map-schemas [left-schema right-schema]
  (let [left-schema-properties (if (map? (second left-schema))
                                 (drop 2 left-schema)
                                 (rest left-schema))
        right-schema-properties (if (map? (second right-schema))
                                  (drop 2 right-schema)
                                  (rest right-schema))
        left-schema (into [] (take (if (map? (second left-schema)) 2 1) left-schema))]
    (into left-schema
          (map (fn [[_ xs]] (last xs)))
          (group-by first (concat right-schema-properties left-schema-properties)))))

(defn- merge-with-parent* [child-schema parent-type types]
  (let [parent-schema (convert-type (get types parent-type) types)]
    (cond
      (= :map (first child-schema) (first parent-schema))
      (merge-map-schemas child-schema parent-schema)

      (and (= :map (first child-schema))
           (= :merge (first parent-schema)))
      (let [parent-schema' (second parent-schema)]
        (into [:merge (merge-map-schemas child-schema parent-schema')]
              (drop 2 parent-schema)))

      :else
      (throw (ex-info "" {:child child-schema
                          :parent parent-schema})))))

(def ^:private merge-with-parent
  (memoize merge-with-parent*))

(defn- behavior->schema [{:keys [type generics] :as _} types]
  (case type
    (:_spec_utils/CommonQueryParameters :_spec_utils/CommonCatQueryParameters)
    [:map [:query (convert-type (get types type) types)]]

    :_spec_utils/AdditionalProperties
    (into [:map-of] (map #(convert-type % types)) generics)

    :_spec_utils/AdditionalProperty
    (into [:map-of {:max 1}] (map #(convert-type % types)) generics)

    :_spec_utils/OverloadOf
    nil

    (throw (ex-info "" _))))

(defn- add-behaviors [schema behaviors types]
  (if-let [behaviors (sequence
                      (comp (map #(behavior->schema % types))
                            (filter identity))
                      behaviors)]
    (into [:merge schema] behaviors)
    schema))

(defmethod -convert-type "interface" [{:keys [properties shortcutProperty
                                             docUrl docId description esQuirk
                                             inherits behaviors] :as _}
                                     types]
  (when (and (not (contains? _ :specLocation))
             (contains? _ :generics))
    (throw (ex-info "" _)))
  (if (empty? (dissoc _ :specLocation :properties :name :kind :description
                      :docId :docUrl :inherits :variantName :attachedBehaviors :behaviors
                      :generics :variants :shortcutProperty :esQuirk :deprecation))
    (cond-> (convert-type {:kind "properties" :properties properties} types)

      (or docUrl docId description esQuirk)
      (add-props (cond-> {}
                   docId (assoc :docId docId)
                   docUrl (assoc :docUrl docUrl)
                   description (assoc :description description)
                   esQuirk (assoc :esQuirk esQuirk)))

      inherits
      (merge-with-parent (:type inherits) types)

      (seq behaviors)
      (add-behaviors behaviors types)

      shortcutProperty
      (make-shortcut shortcutProperty))
    (throw (ex-info "" _))))

(defmethod -convert-type "instance_of" [{:keys [type generics] :as _} types]
  (if (empty? (dissoc _ :kind :type :generics))
    (if (seq generics)
      (let [{ref-generics :generics} (get types type)
            registry (zipmap ref-generics (map #(convert-type % types) generics))]
        [:schema {:registry registry} [:ref type]])
      (case type
        :_builtins/string :string
        :_builtins/boolean :boolean
        (:_builtins/null :_builtins/void) 'nil?
        :_builtins/binary 'bytes?
        :_builtins/number 'number?

        [:ref type]))
    (throw (ex-info "" _))))

(defmethod -convert-type "array_of" [{:keys [value] :as _} types]
  (if (empty? (dissoc _ :value :kind))
    [:sequential (convert-type value types)]
    (throw (ex-info "" _))))

(defn- type->map-entries [{:keys [name type required description aliases
                                  docId docUrl serverDefault since deprecation
                                  esQuirk] :as _}
                          types]
  (if (empty? (dissoc _
                      :name :type :required :description :aliases :docId :docUrl :esQuirk
                      :serverDefault :since :deprecation :codegenName :stability :containerProperty))
    (map (fn [name]
           (add-props
            [name
             (convert-type type types)]
            (cond-> {}
              description (assoc :description description)
              (not required) (assoc :optional true)
              docId (assoc :docId docId)
              docUrl (assoc :docUrl docUrl)
              (some? serverDefault) (assoc :default serverDefault)
              since (assoc :added since)
              deprecation (assoc :deprecated deprecation)
              esQuirk (assoc :esQuirk esQuirk))))
         (cons name aliases))
    (throw (ex-info "" _))))

(defmethod -convert-type "properties" [{:keys [properties] :as _} types]
  (if (empty? (dissoc _ :kind :properties))
    (into [:map] (mapcat #(type->map-entries % types) properties))
    (throw (ex-info "" _))))

(defmethod -convert-type "value" [{:keys [value] :as _} types]
  (if (empty? (dissoc _ :kind :value :codegenName))
    (convert-type value types)
    (throw (ex-info "" _))))

(defmethod -convert-type "response" [{:keys [body] :as _} types]
  (when (and (not (contains? _ :specLocation))
             (contains? _ :generics))
    (throw (ex-info "" _)))
  ;; TODO exceptions!!!, generics (?)
  (if (empty? (dissoc _ :body :specLocation :name :kind :generics :exceptions))
    (cond-> [:map]
      (and (not= body {:kind "no_body"}) (seq body))
      (conj [:body (convert-type body types)]))
    (throw (ex-info "" _))))

(defmethod -convert-type "request" [{:keys [path query body attachedBehaviors] :as _} types]
  (when (and (not (contains? _ :specLocation))
             (contains? _ :generics))
    (throw (ex-info "" _)))
  ;; TODO attachedBehaviors, inherits, deprecation, generics (?)
  (let [attachedBehaviors (set attachedBehaviors)]
    (if (empty? (dissoc _ :description :specLocation :name :kind :query :body :path :attachedBehaviors :inherits :generics :deprecation))
      (cond-> [:map]

        (seq path)
        (conj [:path (into [:map] (mapcat #(type->map-entries % types) path))])

        (seq query)
        (conj [:query (cond-> (into [:map] (mapcat #(type->map-entries % types) query))
                        (contains? attachedBehaviors "CommonQueryParameters")
                        (merge-map-schemas (convert-type (get types :_spec_utils/CommonQueryParameters) types))

                        (contains? attachedBehaviors "CommonCatQueryParameters")
                        (merge-map-schemas (convert-type (get types :_spec_utils/CommonCatQueryParameters) types)))])

        (and (not= body {:kind "no_body"}) (seq body))
        (conj [:body (convert-type body types)]))
      (throw (ex-info "" _)))))

(defmethod -convert-type "union_of" [{:keys [items] :as _} types]
  (if (empty? (dissoc _ :kind :items))
    (into [:or]
          (map #(convert-type % types))
          items)
    (throw (ex-info "" _))))

(defmethod -convert-type "enum" [{:keys [members esQuirk isOpen] :as _} _types]
  (if (empty? (dissoc _ :specLocation :name :kind :members :esQuirk :isOpen))
    ((if isOpen
       (fn [t] [:or t :any])
       identity)
     (into [:enum]
           (sequence
            (comp (map :name)
                  (map #(case [% (some? esQuirk)]
                          ["true" true] true
                          ["false" true] false
                          %)))
            members)))
    (throw (ex-info "" _))))

(defmethod -convert-type "dictionary_of" [{:keys [key value singleKey] :as _} types]
  (if (empty? (dissoc _ :kind :singleKey :key :value))
    (let [k (convert-type key types)
          v (convert-type value types)]
      (cond-> [:map-of]
        singleKey (conj {:min 1 :max 1})
        true (conj k v)))
    (throw (ex-info "" _))))

(defmethod -convert-type "user_defined_value" [{:as _} _types]
  (if (empty? (dissoc _ :kind))
    :any
    (throw (ex-info "" _))))

(defmethod -convert-type "type_alias" [{:keys [type specLocation] :as _} types]
  ;; TODO variants might be important (multispec)
  (if (empty? (dissoc _ :type :kind :specLocation :name :codegenNames :description :generics :docUrl :docId :variants))
    (if specLocation
      (convert-type type types)
      (throw (ex-info "" _)))
    (throw (ex-info "" _))))

(defmethod -convert-type "literal_value" [{:keys [value] :as _} _types]
  (if (empty? (dissoc _ :kind :value))
    [:= value]
    (throw (ex-info "" _))))

(defn- simplify-type [schema name]
  (case name
    :_types/long 'int?
    :_types/double [:or 'zero? 'double?]
    :_types/integer 'int?
    :_types/ulong [:or 'zero? 'pos-int?]
    :_types/short 'int?
    :_types/byte 'int?
    :_types/float [:or 'zero? 'double?]

    schema))

(defn convert-types [types]
  (reduce-kv
   (fn [acc name type]
     (assoc acc name (-> type
                         (convert-type types)
                         (simplify-type name))))
   {}
   types))

(defn normalize-names [schema]
  (walk/prewalk
   (fn [v]
     (if (and (map? v)
              (contains? v :name)
              (contains? v :namespace)
              (string? (:name v))
              (string? (:namespace v))
              (empty? (dissoc v :name :namespace)))
       (keyword (:namespace v) (:name v))
       v))
   schema))

(defn find-free-types [{:keys [types] :as schema}]
  (assoc schema :free-types (into {} (for [[_ {:keys [generics]}] types
                                           :when (seq generics)
                                           generic generics]
                                       [generic :any]))))

(defn urls->http [urls]
  (reduce
   (fn [f {:keys [path methods]}]
     (let [required-keys
           (sequence
            (comp (map second)
                  (distinct))
            (re-seq #"\{([\w\._]+)\}" path))]
       (fn [req]
         (if (every? #(contains? (:path req) %) required-keys)
           (assoc (dissoc req :path)
                  :uri (reduce
                        (fn [path k]
                          (string/replace path
                                          (re-pattern (str "\\{" k "\\}"))
                                          (let [v (get-in req [:path k])]
                                            (if (sequential? v)
                                              (string/join "," v)
                                              v))))
                        path
                        required-keys)
                  :request-method (first methods))
           (f req)))))
   (fn [req]
     (throw (ex-info "" req)))
   urls))

(defn request->validator [request registry]
  (if request
    (let [http-req-schema
          (m/schema (get registry request)
                    {:registry registry})
          req-mapping
          (reduce
           (fn [acc k]
             (reduce
              (fn [acc k']
                (assoc acc k' k))
              acc
              (-> http-req-schema
                  (mu/get k)
                  (mu/keys))))
           {}
           ;; if parameter can be added to any request part then prefer to map it into :body
           (sort-by {:path 0 :query 1 :body 2} (mu/keys http-req-schema)))
          req-schema
          (m/schema (into [:map]
                          (mapcat (fn [[k v]]
                                    (rest (m/-form (mu/select-keys (mu/get http-req-schema k)
                                                                   (map first v)))))
                                  (group-by second req-mapping)))
                    {:registry registry})
          explain (comp me/humanize (m/explainer req-schema))]
      (fn [req]
        (if (m/validate req-schema req)
          (reduce-kv
           (fn [acc k v]
             (assoc-in acc [(req-mapping k) k] v))
           {}
           req)
          (throw (ex-info "" {:errors (explain req)})))))
    (fn [req]
      (if (m/validate 'empty? req)
        {}
        (throw (ex-info "" {:errors (->> req (m/explain 'empty?) (me/humanize))}))))))

(defn instrument-endpoint [{:keys [request response urls] :as endpoint} registry connection]
  (let [req->http-req
        (comp
         (urls->http urls)
         (request->validator request registry))]
    (reify
      client/Client
      (request [_ req]
        (client/request connection (req->http-req req)))
      (request [_ req respond raise]
        (client/request connection (req->http-req req) respond raise)))))

(defn walk-endpoints [endpoints f]
  (walk/postwalk
   (fn [v]
     (if (and (map? v)
              (contains? v :request))
       (f v)
       v))
   endpoints))

(defn instrument-endpoints [{:keys [free-types types] :as x} connection]
  (let [registry (merge (m/default-schemas) (mu/schemas) free-types types)]
    (update x :endpoints walk-endpoints #(instrument-endpoint % registry connection))))

(defn schema->malli [file connection]
  (-> file
      (io/file)
      (json/read-value json/keyword-keys-object-mapper)
      (dissoc :_info)
      (normalize-names)
      (update :types (fn [types]
                       (into {}
                             (map (fn [[k [v]]] [k v]))
                             (group-by :name types))))
      (find-free-types)
      (update :types convert-types)
      (update :endpoints (fn [endpoints]
                           (transduce
                            (map (fn [[k [v]]]
                                   [(string/split k #"\.") v]))
                            (fn
                              ([acc] acc)
                              ([acc [path m]]
                               (assoc-in acc path m)))
                            {}
                            (group-by :name endpoints))))
      (instrument-endpoints connection)))

  (defn search-refs
    ([ref opts] (search-refs #{ref} ref opts))
    ([acc ref opts]
     (transduce
      (comp (map :schema)
            (filter #(satisfies? m/RefSchema %))
            (map m/-ref)
            (distinct)
            (keep identity)
            (filter (complement acc)))
      (fn
        ([] acc)
        ([acc] acc)
        ([acc ref]
         (search-refs (conj acc ref) ref opts)))
      (->> (m/schema [:schema opts ref]
                     {:registry (merge (m/default-schemas) (mu/schemas))})
           (mu/subschemas)))))

  (defn visualize [ref free-types types]
    (let [types (merge free-types types)
          refs (search-refs ref {:registry types})]
      (md/transform
       [:schema {:registry (select-keys types refs)} ref]
       {:registry (merge (m/default-schemas) (mu/schemas))})))

(comment

  (import '(io.micrometer.prometheus PrometheusMeterRegistry PrometheusConfig))

  (let [pmr (PrometheusMeterRegistry. PrometheusConfig/DEFAULT)
        connection (rest/connect "http://localhost:9207/"
                                 {:auth {:type :basic-auth
                                         :params {:user "elastic"
                                                  :pwd "ductile"}}
                                  ;; :threads {:io-dispatch 5}
                                  :connections {:max-per-route 100
                                                :max-total 100}
                                  :metrics {:registry pmr
                                            :tags {:app "SAMPLE"}
                                            :name "ESClient"}})
        {:keys [types free-types endpoints]} (schema->malli (io/resource "./schema.json") connection)]
    (def metrics pmr)
    (def types types)
    (def free-types free-types)
    (def endpoints endpoints)
    (def connection connection))

  (let [cluster-health-api (get-in endpoints ["cluster" "health"])
        search-api (get-in endpoints ["search"])
        x (java.util.concurrent.CountDownLatch. 100000)]
    (client/request cluster-health-api {})
    (time
     (do
       (time
        (dotimes [_ 100000]
          (client/request search-api {"q" "test"}
                          (fn [_res]
                            (.countDown x))
                          (fn [_err]
                            (.countDown x)))))
       (.await x))))

  (.close connection)

  (println
   (.scrape metrics))

  (spit
   "/Users/kchernys/Downloads/graph.dot"
   (visualize :_global.search._types/Hit free-types types) )

  )
