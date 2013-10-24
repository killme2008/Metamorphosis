(ns com.github.killme2008.metamorphosis.dashboard.stats
  (:import [java.net Socket InetSocketAddress]
           [java.io BufferedReader IOException]
           [com.taobao.metamorphosis.server.utils MetaConfig])
  (:require [clojure.java.io :as io]
            [clojure.core.cache :as cache]
            [clojure.string :as str]))

(defn try-read-line [^BufferedReader rdr]
  (try
    (.readLine rdr)
    (catch IOException _
      nil)))

(defn read-lines
  "Like clojure.core/line-seq but opens f with reader.  Automatically
  closes the reader AFTER YOU CONSUME THE ENTIRE SEQUENCE."
  [rdr]
  (let [read-lines (fn this [lines ^BufferedReader rdr]
                     (if-let [line (try-read-line rdr)]
                       (this (conj lines line) rdr)
                       lines))
        lines (read-lines [] rdr)]
    (str/join "\r\n" (rest lines))))

(defn ^{:tag String} stats
  ([host port]
     (stats host port ""))
  ([host port item]
     (let [^Socket s (doto (Socket.) (.setSoTimeout 2000) (.setTcpNoDelay true) (.setSoLinger true 0))]
       (.connect s (InetSocketAddress. host port))
       (with-open [w (io/writer s)
                   r (io/reader s :encoding "utf-8")]
         (.write w (str "stats " item "\r\nquit\r\n"))
         (.flush w)
         (read-lines r)))))

(defonce ^:private meta-config-cache (atom (cache/ttl-cache-factory {} :ttl 30000)))

(defn- load-meta-confg [host port]
  (let [config (stats host port "config")
        ^MetaConfig rt (MetaConfig.)]
    (.loadFromString rt config)
    rt))

(defn ^{:tag MetaConfig} get-meta-config [host port]
  (let [k (str host ":" port)]
    (if (cache/has? @meta-config-cache k)
      (do (swap! meta-config-cache cache/hit k)
          (cache/lookup @meta-config-cache k))
      (let [c (load-meta-confg host port)]
        (swap! meta-config-cache cache/miss k c)
        c))))