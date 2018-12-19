(ns cmr.nlp.elastic.client.search
  (:require
   [taoensso.timbre :as log])
  (:import
   (java.lang String)
   (org.elasticsearch.action.search SearchRequest)
   (org.elasticsearch.index.query QueryBuilders)
   (org.elasticsearch.common.xcontent XContentType)
   (org.elasticsearch.search.builder SearchSourceBuilder))
  (:refer-clojure :exclude [get]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;   Utility Functions   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn search-req
  [& index-names]
  (if (nil? index-names)
    (new SearchRequest)
    (new SearchRequest (into-array index-names))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;   Search API Wrappers   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn fuzzy
  [index-name field-name term]
  (let [builder (new SearchSourceBuilder)
        req (search-req index-name)]
    (.query builder (QueryBuilders/fuzzyQuery field-name term))
    (.source req builder)
    req))

(defn match-all
  [index-name]
  (let [builder (new SearchSourceBuilder)
        req (search-req index-name)]
    (.query builder (QueryBuilders/matchAllQuery))
    (.source req builder)
    req))

(defn match
  [index-name field-name term]
  (let [builder (new SearchSourceBuilder)
        req (search-req index-name)]
    (.query builder (QueryBuilders/matchQuery field-name term))
    (.source req builder)
    req))

(defn regex
  [index-name field-name re]
  (let [builder (new SearchSourceBuilder)
        req (search-req index-name)]
    (.query builder (QueryBuilders/regexpQuery field-name re))
    (.source req builder)
    req))

(defn term
  [index-name field-name term]
  (let [builder (new SearchSourceBuilder)
        req (search-req index-name)]
    (.query builder (QueryBuilders/termQuery field-name term))
    (.source req builder)
    req))

(defn wildcard
  [index-name field-name query]
  (let [builder (new SearchSourceBuilder)
        req (search-req index-name)]
    (.query builder (QueryBuilders/wildcardQuery field-name query))
    (.source req builder)
    req))
