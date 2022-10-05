(ns avisi.xtdb.xodus
  (:require [xtdb.kv :as kv]
            [xtdb.system :as sys]
            [xtdb.memory :as mem])
  (:import [java.io Closeable]
           [jetbrains.exodus.env Environments Environment Store StoreConfig Transaction Cursor TransactionalExecutable]
           [jetbrains.exodus ArrayByteIterable ByteIterable]))

(set! *warn-on-reflection* true)

(defn with-transaction!
  "Calls the given function with a transaction.
  Will revert all changes on exception and throw the exception"
  ([^Environment env func]
   (let [computable (reify TransactionalExecutable
                      (^void execute [this ^Transaction tx]
                        (func tx)))]
     (.executeInTransaction env computable))))

(defn byte-iterable->key [^ByteIterable b]
  (when b
    (mem/on-heap-buffer ^bytes (.getBytesUnsafe b))))

(defn ^ByteIterable key->byte-iterable [k]
  (ArrayByteIterable. (mem/->on-heap k)))

(defrecord XodusKvIterator [^Cursor cursor]
  kv/KvIterator
  (seek [this k]
    (when-let [key (.getSearchKeyRange cursor (key->byte-iterable k))]
      (byte-iterable->key (.getKey cursor))))
  (next [this]
    (when (.getNext cursor)
      (byte-iterable->key (.getKey cursor))))
  (prev [this]
    (when (.getPrev cursor)
      (byte-iterable->key (.getKey cursor))))
  (value [this]
    (byte-iterable->key (.getValue cursor)))
  Closeable
  (close [this]
    (.close ^Cursor cursor)))

(defrecord XodusKvSnapshot [^Transaction tx ^Store store]
  kv/KvSnapshot
  (new-iterator [this]
    (->XodusKvIterator (.openCursor store tx)))
  (get-value [this k]
    (byte-iterable->key (.get ^Store store ^Transaction tx (key->byte-iterable k))))
  Closeable
  (close [this]
    (.abort ^Transaction tx)))

(defrecord XodusKv [db-dir ^Store store ^Environment env]
  kv/KvStore
  (new-snapshot [{:keys [^Environment env]}]
    (let [tx ^Transaction (.beginReadonlyTransaction env)]
      (->XodusKvSnapshot tx store)))
  (store [{:keys [^Environment env]} kvs]
    (with-transaction! env (fn [^Transaction tx]
                             (doseq [[^bytes k ^bytes v] kvs]
                               (let [k-bytes (ArrayByteIterable. (mem/->on-heap k))]
                                 (if-let [v-bytes (some-> v mem/->on-heap ArrayByteIterable.)]
                                   (.put store tx k-bytes v-bytes)
                                   (.delete store tx k-bytes)))))))
  (compact [_]
    ;; Maybe we could call .gc on the env, but that won't do what they want I think
    )
  (fsync [_]
    ;; This is not a thing in Xodus
    )
  (count-keys [{:keys [^Environment env]}]
    (let [tx ^Transaction (.beginReadonlyTransaction env)]
      (try
        (.count store tx)
        (finally
          (.abort tx)))))
  (db-dir [_]
    (str db-dir))
  (kv-name [this]
    (.getName (class this)))
  Closeable
  (close [{:keys [env]}]
    (.executeTransactionSafeTask
      ^Environment env
      ^Runnable
      (reify
        Runnable
        (run [_]
          (.close ^Environment env))))))

(defn ->kv-store {::sys/args {:db-dir {:doc "Directory to store K/V files"
                                       :required? true
                                       :spec ::sys/path}}}
  [{:keys [db-dir]}]
  (let [env ^Environment (Environments/newInstance ^String (str db-dir))
        store (atom nil)]
    (with-transaction! env (fn [^Transaction tx]
                             (reset! store (.openStore env "kv" StoreConfig/WITHOUT_DUPLICATES tx))))
    (map->XodusKv
      {:store @store
       :db-dir db-dir
       :env env})))
