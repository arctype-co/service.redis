(ns ^{:doc "Redis client driver using Carmine"}
  arctype.service.io.redis
  (:refer-clojure :exclude [keys sync eval set type time get sort])
  (:require
    [clojure.tools.logging :as log]
    [schema.core :as S]
    [sundbry.resource :as resource]
    [taoensso.carmine :as carmine]
    [taoensso.carmine.commands :as commands]
    [arctype.service.protocol :refer :all]))

(def Config
  {:host S/Str
   (S/optional-key :port) S/Int
   (S/optional-key :password) S/Str
   (S/optional-key :timeout-ms) S/Int
   (S/optional-key :db) S/Int})

(def default-config
  {:port 6379})

(defmacro wcar
  [this & body]
  `(carmine/wcar 
     (:conn ~this)
     ~@body))

(commands/defcommands)

(defn new-pubsub-listener
  [{{spec :spec} :conn :as this} channel-handlers] 
  (let [pubsub-conn-spec (dissoc spec :timeout)]
    (carmine/with-new-pubsub-listener
      pubsub-conn-spec
      channel-handlers
      (doseq [channel (clojure.core/keys channel-handlers)]
        (log/debug {:message "Subscribing to redis pubsub channel"
                    :channel channel})
        (carmine/subscribe channel)))))

(defn close-pubsub-listener
  [listener]
  (log/debug {:message "Closing redis pubsub channel"})
  (carmine/close-listener listener))

(defrecord RedisClient [config conn]
  PLifecycle
  (start [this]
    (-> this
        (assoc :conn {:pool {}
                      :spec config})))

  (stop [this]
    (-> this
        (dissoc :conn)))
  
  )

(S/defn create
  [resource-name
   config :- Config]
  (let [config (merge default-config config)]
    (resource/make-resource
      (map->RedisClient
        {:config config})
      resource-name)))
