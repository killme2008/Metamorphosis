(ns com.github.killme2008.metamorphosis.dashboard.handler
  (:use compojure.core)
  (:use [ring.velocity.core :only [render]])
  (:require [compojure.handler :as handler]
            [compojure.route :as route]))

(defonce broker-ref (atom nil))

(defn- render-tpl [tpl & vs]
  (apply render (str "templates/" tpl) vs))

(defn-  index [req]
  (render-tpl "index.vm"))

(defroutes app-routes
  (GET "/" [] index)
  (route/resources "/")
  (route/not-found "Not Found"))

(def app
  (handler/site app-routes))