(ns com.github.killme2008.metamorphosis.dashboard.handler
  (:use compojure.core)
  (:require [compojure.handler :as handler]
            [compojure.route :as route]))

(defonce broker-ref (atom nil))

(defroutes app-routes
  (GET "/" [] (str (-> @broker-ref (.getMetaConfig))))
  (route/not-found "Not Found"))

(def app
  (handler/site app-routes))