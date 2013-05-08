(ns com.github.killme2008.metamorphosis.dashboard.handler
  (:use compojure.core)
  (:use [ring.velocity.core :only [render]]
        [environ.core])
  (:import [org.apache.log4j Logger])
  (:require [compojure.handler :as handler]
            [clojure.java.io :as io]
            [com.github.killme2008.metamorphosis.dashboard.util :as u]
            [compojure.route :as route]))

(defonce broker-ref (atom nil))

(defmacro with-broker [ & body]
  `(-> @broker-ref ~@body))

(defn- render-tpl [tpl & vs]
  (apply render (str "templates/" tpl) vs))

(defn-  index [req]
  (render-tpl "index.vm" :topics (with-broker (.getStoreManager) (.getAllTopics))))

(defn- instance []
  {:start (u/pretty-time (with-broker (.getStatsManager) (.getStartupTimestamp)))
   :host (with-broker (.getBrokerZooKeeper) (.getBrokerHostName))
   :data (with-broker (.getMetaConfig) (.getDataPath))
   :data_log (with-broker (.getMetaConfig) (.getDataLogPath))
   :cwd (System/getProperty "user.dir")})

(defn- version []
  {:metaq (with-broker (.getStatsManager) (.getVersion))})

(defn- jvm []
  {:runtime (System/getProperty "java.vm.name")
   :processors (-> (Runtime/getRuntime) (.availableProcessors))
   :args (u/vm-args)})

(defn- system []
  (into {}
        (map (fn [[k v]]
               (if-not (contains? #{:fdc :fdc_used} k)
                 [k (u/readable-size v)]
                 [k v]))
             {:sys_memory (u/sys-memory)
              :sys_memory_used (u/sys-memory-used)
              :swap_space (u/swap-space)
              :ss_used  (u/swap-space-used)
              :fdc (u/file-descriptors)
              :fdc_used (u/file-descriptors-used)
              :jvm_memory_max (-> (Runtime/getRuntime) (.maxMemory))
              :jvm_memory_total (-> (Runtime/getRuntime) (.totalMemory))
              :jvm_memory_used (- (-> (Runtime/getRuntime) (.totalMemory)) (-> (Runtime/getRuntime) (.freeMemory)))})))

(defn- dashboard [req]
  (render-tpl "dashboard.vm"
              :instance (u/stringfy-map-keys (instance))
              :version (u/stringfy-map-keys (version)) 
              :jvm (u/stringfy-map-keys (jvm))
              :system (u/stringfy-map-keys (system))))

(defn- logging [req]
  (let [timestamp (or (-> req :params :timetamp) 0)
        appender (-> (Logger/getRootLogger) (.getAppender "ServerDailyRollingFile"))]
    (render-tpl "logs.vm" :logs (.getLogs appender timestamp))))

(defn- java-properties [req]
  (render-tpl "java_properties.vm" :props (System/getProperties)))

(defn- thread-dump [req]
  (render-tpl "thread_dump.vm" :threads (u/dump-threads)))

(defn- config [req]
  (with-open [in (io/reader (with-broker (.getMetaConfig) (.getConfigFilePath)))]
    (render-tpl "config.vm"
                :config (slurp in)
                :lastLoaded (u/pretty-time (with-broker (.getMetaConfig) (.getLastModified))))))

(defn- topic-list [req]
  (render-tpl "topics.vm" :topics (with-broker (.getStatsManager) (.getTopicsStats))))

(defn- topic-info [req]
  (let [topic (-> req :params :topic)]
    (render-tpl "topic.vm" :topic topic)))

(defn- reload-config [req]
  (try
    (with-broker (.getMetaConfig) (.reload))
    "Reload config successfully."
    (catch Exception e
      (.getMessage e))))

(defn- cluster [req]
  (render-tpl "cluster.vm"))

(defroutes app-routes
  (GET "/" [] index)
  (GET "/dashboard" [] dashboard)
  (GET "/logging" [] logging)
  (GET "/cluster" [] cluster)
  (GET "/java-properties" [] java-properties)
  (GET "/thread-dump" [] thread-dump)
  (GET "/config" [] config)
  (POST "/reload-config" [] reload-config)
  (GET "/topic-list" [] topic-list)
  (GET "/topic/:topic" [] topic-info)
  (route/resources "/")
  (route/not-found "Not Found"))

(def app
  (handler/site app-routes))