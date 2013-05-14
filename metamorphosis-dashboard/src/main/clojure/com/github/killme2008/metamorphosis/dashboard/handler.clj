(ns com.github.killme2008.metamorphosis.dashboard.handler
  (:use compojure.core)
  (:use [ring.velocity.core :only [render]]
        [environ.core])
  (:import [org.apache.log4j Logger]
           [org.I0Itec.zkclient ZkClient]
           [com.taobao.metamorphosis.utils ZkUtils]
           [com.taobao.metamorphosis.server.store MessageStoreManager MessageStore]
           [com.taobao.metamorphosis.utils MetaZookeeper MetaZookeeper$ZKGroupTopicDirs])
  (:require [compojure.handler :as handler]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [com.github.killme2008.metamorphosis.dashboard.stats :as stats]
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
  {:metaq (with-broker (.getStatsManager) (.getVersion))
   :id (with-broker (.getMetaConfig) (.getBrokerId))
   :uri (with-broker (.getBrokerZooKeeper) (.getBroker))})

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

(defn- query-pending-messages [topic-stats group]
  (let [^String topic (.getTopic topic-stats)
        avg-msg-size (if (> (.getMessageCount topic-stats) 0)
                       (/ (.getMessageBytes topic-stats) (.getMessageCount topic-stats))
                       "N/A")
        ^MetaZookeeper mz (with-broker (.getBrokerZooKeeper) (.getMetaZookeeper))
        ^ZkClient zc (.getZkClient mz)
        broker-id (with-broker (.getMetaConfig) (.getBrokerId))
        ^MetaZookeeper$ZKGroupTopicDirs topicDirs (MetaZookeeper$ZKGroupTopicDirs. mz topic group)
        ^MessageStoreManager msm (with-broker (.getStoreManager))]
    (if (ZkUtils/pathExists zc (.consumerGroupDir topicDirs))
      (vec (map (fn [partition]
                  (merge {"partition" partition}
                         (let [part-str (str broker-id "-" partition)
                               offset-znode (str (.consumerOffsetDir topicDirs) "/" part-str)
                               owner-znode (str (.consumerOwnerDir topicDirs) "/" part-str)
                               offset-str (ZkUtils/readDataMaybeNull zc offset-znode)]
                           (when offset-str
                             (let [consumer-offset (Integer/valueOf
                                                    (if (.contains offset-str "-")
                                                      (second (.split offset-str "-"))
                                                      offset-str))
                                   max-offset (-> msm (.getMessageStore topic partition) (.getMaxOffset))
                                   min-offset (-> msm (.getMessageStore topic partition) (.getMinOffset))
                                   pending-bytes (- max-offset consumer-offset)
                                   consumed-bytes (- consumer-offset min-offset)
                                   pending-messages (if-not (= avg-msg-size "N/A")
                                                      (long (quot pending-bytes  avg-msg-size))
                                                      "N/A")
                                   consumed-messages (if-not (= avg-msg-size "N/A")
                                                      (long (quot consumed-bytes  avg-msg-size))
                                                      "N/A")]
                               {"pending-bytes" pending-bytes
                                "pending-messages" pending-messages
                                "consumed-bytes" consumed-bytes
                                "consumed-messages" consumed-messages
                                "owner" (ZkUtils/readDataMaybeNull zc owner-znode)}
                               )))))
                (range 0 (.getPartitions topic-stats))))  
      {"error" (format "The consumer group <strong>'%s'</strong> is not exists." group)})))

(defn- topic-info [req]
  (let [topic (-> req :params :topic)
        topic-stats (with-broker (.getStatsManager) (.getTopicStats topic))
        group (-> req :params :group)]
    (if-not (empty? group)
      (render-tpl "topic.vm" :topic topic-stats :group group
                  :pending-stats (query-pending-messages topic-stats group))
      (render-tpl "topic.vm" :topic topic-stats))))

(defn- reload-config [req]
  (try
    (with-broker (.getMetaConfig) (.reload))
    "Reload config successfully."
    (catch Exception e
      (log/error "Reload config failed" e)
      (.getMessage e))))

(defn- stats [req]
  (let [item (-> req :params :item)]
    (render-tpl "stats.vm"
                :result
                (with-broker (.getStatsManager) (.getStatsInfo item)))))

(defn- cluster [req]
  (let [ ^MetaZookeeper mz (with-broker (.getBrokerZooKeeper) (.getMetaZookeeper))
        cluster (.getCluster mz)
        current-broker (with-broker (.getBrokerZooKeeper) (.getBroker))
        all-brokers (.getBrokers cluster)]
    (render-tpl "cluster.vm" :current current-broker
                :nodes
                (vec
                 (map (fn [[id brokers]]
                        {"id" id
                         "brokers"
                         (map
                          (fn [broker]
                            (when-let [broker-str (str broker)]
                              (let [uri (java.net.URI. broker-str)
                                    host (.getHost uri)
                                    broker-port (.getPort uri)]
                                {"dashboard-uri" (str "http://" host ":" (-> (stats/get-meta-config host broker-port) (.getDashboardHttpPort)))
                                 "slave" (.isSlave broker)
                                 "broker" broker
                                 "broker-uri" broker-str})))
                          brokers)})
                      all-brokers)))))

(defn not-found []
  {:status 200
   :body (render-tpl "not_found.vm")})

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
  (GET "/stats" [] stats)
  (GET "/stats/:item" [] stats)
  (route/resources "/"))

(defn wrap-error-handler [handler]
  (fn [req]
    (try
      (or (handler req) (not-found))
      (catch Exception e
        (log/error "Process request failed" e)
        {:status 200
         :body (render-tpl "error.vm" :error (or (.getMessage e) e))}))))

(def app
  (handler/site (-> app-routes wrap-error-handler)))