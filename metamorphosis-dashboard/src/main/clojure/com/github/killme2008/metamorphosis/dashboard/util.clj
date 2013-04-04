(ns com.github.killme2008.metamorphosis.dashboard.util
  (:import [org.ocpsoft.prettytime PrettyTime]
           [com.sun.management UnixOperatingSystemMXBean]
           [java.lang.management ManagementFactory RuntimeMXBean OperatingSystemMXBean])
  (:require [clojure.string :as str]))

(defn stringfy-map-keys [m]
  (zipmap (map name (keys m)) (vals m)))

(defn pretty-time [ts]
  (let [^PrettyTime p (PrettyTime.)
        date (java.util.Date. ts)]
    (.format p date)))

(defn vm-args []
  (str/join " " (-> (ManagementFactory/getRuntimeMXBean) (.getInputArguments))))

(defn sys-memory []
  (-> (ManagementFactory/getOperatingSystemMXBean) (.getTotalPhysicalMemorySize)))

(defn sys-memory-used []
  (-
   (sys-memory)
   (-> (ManagementFactory/getOperatingSystemMXBean) (.getFreePhysicalMemorySize))))

(defn swap-space []
  (-> (ManagementFactory/getOperatingSystemMXBean) (.getTotalSwapSpaceSize)))

(defn swap-space-used []
  (-
   (swap-space)
   (-> (ManagementFactory/getOperatingSystemMXBean) (.getFreeSwapSpaceSize))))

(defn file-descriptors []
  (let [osm (ManagementFactory/getOperatingSystemMXBean)]
    (if (instance? UnixOperatingSystemMXBean osm)
      (.getMaxFileDescriptorCount osm)
      "unknow")))

(defn file-descriptors-used []
  (let [osm (ManagementFactory/getOperatingSystemMXBean)]
    (if (instance? UnixOperatingSystemMXBean osm)
      (.getOpenFileDescriptorCount osm)
      "unknow")))


  