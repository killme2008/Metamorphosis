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

(defn dump-threads []
  (let [tmb (ManagementFactory/getThreadMXBean)]
    (map str (seq (.dumpAllThreads tmb false false)))))

(defonce ^:private units ["B" "KB" "MB" "GB" "TB"])
(defn readable-size [size]
  (if (<= size 0)
    "0"
    (let [digit-groups (int (/ (Math/log10 size) (Math/log10 1024)))]
      (str (-> (java.text.DecimalFormat. "#,##0.#") (.format (/ size (Math/pow 1024 digit-groups))))
           " "
           (nth units digit-groups)))))
