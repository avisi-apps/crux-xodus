(ns avisi.crux.kv.xodus
  (:require [crux.kv :as kv]
            [crux.memory :as mem]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io])
  (:import [java.io Closeable File]
           [jetbrains.exodus.env Environments Environment Store StoreConfig Transaction Cursor TransactionalExecutable]
           [jetbrains.exodus ArrayByteIterable ByteIterable]
           [jetbrains.exodus.util CompressBackupUtil]
           [jetbrains.exodus.backup BackupStrategy VirtualFileDescriptor]))

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
    (.close cursor)))

(defrecord XodusKvSnapshot [^Transaction tx ^Store store]
  kv/KvSnapshot
  (new-iterator [this]
    (->XodusKvIterator (.openCursor store tx)))
  (get-value [this k]
    (byte-iterable->key (.get ^Store store ^Transaction tx (key->byte-iterable k))))
  Closeable
  (close [this]
    (.abort ^Transaction tx)))

(s/def ::options (s/keys :req-un [:crux.kv/db-dir]
                         :opt-un [:crux.kv/sync?]))

(defrecord XodusKv [db-dir ^Store store env]
  kv/KvStore
  (open [this {:keys [db-dir] :as options}]
    (s/assert ::options options)
    (let [env ^Environment (Environments/newInstance ^String db-dir)
          store (atom nil)]
      (with-transaction! env (fn [^Transaction tx]
                               (reset! store (.openStore env "kv" StoreConfig/WITHOUT_DUPLICATES tx))))
      (assoc this
             :store @store
             :db-dir db-dir
             :env env)))
  (new-snapshot [{:keys [^Environment env]}]
   (let [tx ^Transaction (.beginReadonlyTransaction env)]
     (->XodusKvSnapshot tx store)))
  (store [{:keys [^Environment env]} kvs]
    (with-transaction! env (fn [^Transaction tx]
                             (doseq [[k v] kvs]
                               (.put store tx (ArrayByteIterable. (mem/->on-heap ^bytes k)) (ArrayByteIterable. (mem/->on-heap ^bytes v)))))))
  (delete [{:keys [^Environment env]} ks]
    (with-transaction! env (fn [^Transaction tx]
                             (doseq [k ks]
                               (.delete store tx (ArrayByteIterable. (mem/->on-heap ^bytes k)))))))
  (fsync [{:keys [^Environment env]}])
  (backup [{:keys [^Environment env]} dir]
    (let [strategy ^BackupStrategy (.getBackupStrategy env)]
      (.beforeBackup strategy)
      (run!
       (fn [^VirtualFileDescriptor fd]
         (let [file-size (Math/min (.getFileSize fd) (.acceptFile strategy fd))]
           (when (> file-size 0)
             (let [out-file ^File (File. ^File dir (str (.getPath fd) (.getName fd)))]
               (io/make-parents out-file)
               (io/copy (.getInputStream fd) (io/file out-file))
               (.setLastModified out-file (.getTimeStamp fd))))))
       (seq (.getContents strategy)))
      (.afterBackup strategy)))
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
  (close [{:keys [^Environment env]}]
   (.close env)))


