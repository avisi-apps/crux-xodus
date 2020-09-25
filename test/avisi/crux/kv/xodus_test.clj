(ns avisi.crux.kv.xodus-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [crux.codec :as c]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [crux.io :as cio]
            [avisi.crux.xodus :as xodus]
            [crux.system :as sys]
            [crux.api :as crux]
            [clojure.java.io :as io])
  (:import [org.agrona.concurrent UnsafeBuffer]
           [java.nio ByteOrder]))

(def ^:dynamic *kv*)
(def ^:dynamic *kv-opts* {:crux/module `xodus/->kv-store, :db-dir-suffix "xodus"})

(defn with-kv-store* [f]
  (let [db-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [kv (-> (sys/prep-system
                           {:kv-store (merge (when-let [db-dir-suffix (:db-dir-suffix *kv-opts*)]
                                               {:db-dir (io/file db-dir db-dir-suffix)})
                                             *kv-opts*)})
                       (sys/start-system)
                       )]
        (binding [*kv* (:kv-store kv)]
          (f *kv*)))
      (finally
        (cio/delete-dir db-dir)))))

(defmacro with-kv-store [bindings & body]
  `(with-kv-store* (fn [~@bindings] ~@body)))

(defn seek [kvs k]
  (with-open [snapshot (kv/new-snapshot kvs)
              i (kv/new-iterator snapshot)]
    (when-let [k (kv/seek i k)]
      [(mem/->on-heap k) (mem/->on-heap (kv/value i))])))

(defn value [kvs seek-k]
  (with-open [snapshot (kv/new-snapshot kvs)]
    (some-> (kv/get-value snapshot seek-k)
      (mem/->on-heap))))

(defn seek-and-iterate [kvs key-pred seek-k]
  (with-open [snapshot (kv/new-snapshot kvs)
              i (kv/new-iterator snapshot)]
    (loop [acc (transient [])
           k (kv/seek i seek-k)]
      (let [k (when k
                (mem/->on-heap k))]
        (if (and k (key-pred k))
          (recur (conj! acc [k (mem/->on-heap (kv/value i))])
            (kv/next i))
          (persistent! acc))))))

(defn long->bytes ^bytes [^long l]
  (let [ub (UnsafeBuffer. (byte-array Long/BYTES))]
    (.putLong ub 0 l ByteOrder/BIG_ENDIAN)
    (.byteArray ub)))

(defn bytes->long ^long [^bytes data]
  (let [ub (UnsafeBuffer. data)]
    (.getLong ub 0 ByteOrder/BIG_ENDIAN)))

(defn compare-bytes
  (^long [^bytes a ^bytes b]
   (mem/compare-buffers (mem/as-buffer a) (mem/as-buffer b)))
  (^long [^bytes a ^bytes b max-length]
   (mem/compare-buffers (mem/as-buffer a) (mem/as-buffer b) max-length)))

(defn bytes=?
  ([^bytes a ^bytes b]
   (mem/buffers=? (mem/as-buffer a) (mem/as-buffer b)))
  ([^bytes a ^bytes b ^long max-length]
   (mem/buffers=? (mem/as-buffer a) (mem/as-buffer b) max-length)))

(t/deftest test-store-and-value []
                                (with-kv-store [kv-store]
                                  (t/testing "store, retrieve and seek value"
                                    (kv/store kv-store [[(long->bytes 1) (.getBytes "Crux")]])
                                    (t/is (= "Crux" (String. ^bytes (value kv-store (long->bytes 1)))))
                                    (t/is (= [1 "Crux"] (let [[k v] (seek kv-store (long->bytes 1))]
                                                          [(bytes->long k) (String. ^bytes v)]))))

                                  (t/testing "non existing key"
                                    (t/is (nil? (value kv-store (long->bytes 2)))))))

(t/deftest test-can-store-and-delete-all-116 []
                                             (with-kv-store [kv-store]
                                               (let [number-of-entries 500]
                                                 (kv/store kv-store (map (fn [i]
                                                                           [(long->bytes i) (long->bytes (inc i))])
                                                                      (range number-of-entries)))
                                                 (doseq [i (range number-of-entries)]
                                                   (t/is (= (inc i) (bytes->long (value kv-store (long->bytes i))))))

                                                 (t/testing "deleting all keys in random order, including non existent keys"
                                                   (kv/delete kv-store (for [i (shuffle (range (long (* number-of-entries 1.2))))]
                                                                         (long->bytes i)))
                                                   (doseq [i (range number-of-entries)]
                                                     (t/is (nil? (value kv-store (long->bytes i)))))))))

(t/deftest test-seek-and-iterate-range []
                                       (with-kv-store [kv-store]
                                         (doseq [[^String k v] {"a" 1 "b" 2 "c" 3 "d" 4}]
                                           (kv/store kv-store [[(.getBytes k) (long->bytes v)]]))

                                         (t/testing "seek range is exclusive"
                                           (t/is (= [["b" 2] ["c" 3]]
                                                   (for [[^bytes k v] (seek-and-iterate kv-store
                                                                        #(neg? (compare-bytes % (.getBytes "d")))
                                                                        (.getBytes "b"))]
                                                     [(String. k) (bytes->long v)]))))

                                         (t/testing "seek range after existing keys returns empty"
                                           (t/is (= [] (seek-and-iterate kv-store #(neg? (compare-bytes % (.getBytes "d"))) (.getBytes "d"))))
                                           (t/is (= [] (seek-and-iterate kv-store #(neg? (compare-bytes % (.getBytes "f")%)) (.getBytes "e")))))

                                         (t/testing "seek range before existing keys returns keys at start"
                                           (t/is (= [["a" 1]] (for [[^bytes k v] (into [] (seek-and-iterate kv-store #(neg? (compare-bytes % (.getBytes "b"))) (.getBytes "0")))]
                                                                [(String. k) (bytes->long v)]))))))

(t/deftest test-seek-between-keys []
                                  (with-kv-store [kv-store]
                                    (doseq [[^String k v] {"a" 1 "c" 3}]
                                      (kv/store kv-store [[(.getBytes k) (long->bytes v)]]))

                                    (t/testing "seek returns next valid key"
                                      (t/is (= ["c" 3]
                                              (let [[^bytes k v] (seek kv-store (.getBytes "b"))]
                                                [(String. k) (bytes->long v)]))))))

(t/deftest test-seek-and-iterate-prefix []
                                        (with-kv-store [kv-store]
                                          (doseq [[^String k v] {"aa" 1 "b" 2 "bb" 3 "bcc" 4 "bd" 5 "dd" 6}]
                                            (kv/store kv-store [[(.getBytes k) (long->bytes v)]]))

                                          (t/testing "seek within bounded prefix returns all matching keys"
                                            (t/is (= [["b" 2] ["bb" 3] ["bcc" 4] ["bd" 5]]
                                                    (for [[^bytes k v] (into [] (seek-and-iterate kv-store #(bytes=? (.getBytes "b") % (alength (.getBytes "b"))) (.getBytes "b")))]
                                                      [(String. k) (bytes->long v)]))))

                                          (t/testing "seek within bounded prefix before or after existing keys returns empty"
                                            (t/is (= [] (into [] (seek-and-iterate kv-store (partial bytes=? (.getBytes "0")) (.getBytes "0")))))
                                            (t/is (= [] (into [] (seek-and-iterate kv-store (partial bytes=? (.getBytes "e")) (.getBytes "0"))))))))

(t/deftest test-delete-keys []
                            (with-kv-store [kv-store]
                              (t/testing "store, retrieve and delete value"
                                (kv/store kv-store [[(long->bytes 1) (.getBytes "Crux")]])
                                (t/is (= "Crux" (String. ^bytes (value kv-store (long->bytes 1)))))
                                (kv/delete kv-store [(long->bytes 1)])
                                (t/is (nil? (value kv-store (long->bytes 1))))
                                (t/testing "deleting non existing key is noop"
                                  (kv/delete kv-store [(long->bytes 1)])))))

(t/deftest test-compact []
                        (with-kv-store [kv-store]
                          (t/testing "store, retrieve and delete value"
                            (kv/store kv-store [[(.getBytes "key-with-a-long-prefix-1") (.getBytes "Crux")]])
                            (kv/store kv-store [[(.getBytes "key-with-a-long-prefix-2") (.getBytes "is")]])
                            (kv/store kv-store [[(.getBytes "key-with-a-long-prefix-3") (.getBytes "awesome")]])
                            (t/testing "compacting"
                              (kv/compact kv-store))
                            (t/is (= "Crux" (String. ^bytes (value kv-store (.getBytes "key-with-a-long-prefix-1")))))
                            (t/is (= "is" (String. ^bytes (value kv-store (.getBytes "key-with-a-long-prefix-2")))))
                            (t/is (= "awesome" (String. ^bytes (value kv-store (.getBytes "key-with-a-long-prefix-3"))))))))


(t/deftest test-can-get-from-snapshot
  (with-kv-store [kv-store]
    (kv/store kv-store [[(long->bytes 1) (.getBytes "Crux")]])
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (t/is (= "Crux" (String. (mem/->on-heap (kv/get-value snapshot (long->bytes 1))))))
      (t/is (nil? (kv/get-value snapshot (long->bytes 2)))))))

(t/deftest test-can-read-write-concurrently
  (with-kv-store [kv-store]
    (let [w-fs (for [_ (range 128)]
                 (future
                   (kv/store kv-store [[(long->bytes 1) (.getBytes "Crux")]])))]
      @(first w-fs)
      (let [r-fs (for [_ (range 128)]
                   (future
                     (String. ^bytes (value kv-store (long->bytes 1)))))]
        (mapv deref w-fs)
        (doseq [r-f r-fs]
          (t/is (= "Crux" @r-f)))))))

(t/deftest test-prev-and-next []
                              (with-kv-store [kv-store]
                                (doseq [[^String k v] {"a" 1 "c" 3}]
                                  (kv/store kv-store [[(.getBytes k) (long->bytes v)]]))

                                (with-open [snapshot (kv/new-snapshot kv-store)
                                            i (kv/new-iterator snapshot)]
                                  (t/testing "seek returns next valid key"
                                    (let [k (kv/seek i (mem/as-buffer (.getBytes "b")))]
                                      (t/is (= ["c" 3] [(String. (mem/->on-heap k)) (bytes->long (mem/->on-heap (kv/value i)))]))))
                                  (t/testing "prev, iterators aren't bidirectional"
                                    (t/is (= "a" (String. (mem/->on-heap (kv/prev i)))))
                                    (t/is (nil? (kv/prev i)))))))

(tcct/defspec test-basic-generative-store-and-get-value 20
  (prop/for-all [kvs (gen/not-empty (gen/map
                                      gen/simple-type-printable
                                      gen/int {:num-elements 100}))]
    (let [kvs (->> (for [[k v] kvs]
                     [(c/->value-buffer k)
                      (c/->value-buffer v)])
                (into {}))]
      (with-kv-store [kv-store]
        (kv/store kv-store kvs)
        (with-open [snapshot (kv/new-snapshot kv-store)]
          (->> (for [[k v] kvs]
                 (= v (kv/get-value snapshot k)))
            (every? true?)))))))

(tcct/defspec test-generative-kv-store-commands 20
  (prop/for-all [commands (gen/let [ks (gen/not-empty (gen/vector gen/simple-type-printable))]
                            (gen/not-empty (gen/vector
                                             (gen/one-of
                                               [(gen/tuple
                                                  (gen/return :get-value)
                                                  (gen/elements ks))
                                                (gen/tuple
                                                  (gen/return :seek)
                                                  (gen/elements ks))
                                                (gen/tuple
                                                  (gen/return :seek+value)
                                                  (gen/elements ks))
                                                (gen/tuple
                                                  (gen/return :seek+nexts)
                                                  (gen/elements ks))
                                                (gen/tuple
                                                  (gen/return :seek+prevs)
                                                  (gen/elements ks))
                                                (gen/tuple
                                                  (gen/return :fsync))
                                                (gen/tuple
                                                  (gen/return :delete)
                                                  (gen/elements ks))
                                                (gen/tuple
                                                  (gen/return :store)
                                                  (gen/elements ks)
                                                  gen/int)]))))]
    (with-kv-store [kv-store]
      (let [expected (->> (reductions
                            (fn [[state] [op k v :as command]]
                              (case op
                                :get-value [state (get state (c/->value-buffer k))]
                                :seek [state (ffirst (subseq state >= (c/->value-buffer k)))]
                                :seek+value [state (second (first (subseq state >= (c/->value-buffer k))))]
                                :seek+nexts [state (subseq state >= (c/->value-buffer k))]
                                :seek+prevs [state (some->> (subseq state >= (c/->value-buffer k))
                                                     (ffirst)
                                                     (rsubseq state <= ))]
                                :fsync [state]
                                :delete [(dissoc state (c/->value-buffer k))]
                                :store [(assoc state
                                               (c/->value-buffer k)
                                               (c/->value-buffer v))]))
                            [(sorted-map-by mem/buffer-comparator)]
                            commands)
                       (rest)
                       (map second))]
        (->> (for [[[op k v :as command] expected] (map vector commands expected)]
               (= expected
                 (case op
                   :get-value (with-open [snapshot (kv/new-snapshot kv-store)]
                                (kv/get-value snapshot (c/->value-buffer k)))
                   :seek (with-open [snapshot (kv/new-snapshot kv-store)
                                     i (kv/new-iterator snapshot)]
                           (kv/seek i (c/->value-buffer k)))
                   :seek+value (with-open [snapshot (kv/new-snapshot kv-store)
                                           i (kv/new-iterator snapshot)]
                                 (when (kv/seek i (c/->value-buffer k))
                                   (kv/value i)))
                   :seek+nexts (with-open [snapshot (kv/new-snapshot kv-store)
                                           i (kv/new-iterator snapshot)]
                                 (when-let [k (kv/seek i (c/->value-buffer k))]
                                   (cons [(mem/copy-to-unpooled-buffer k)
                                          (mem/copy-to-unpooled-buffer (kv/value i))]
                                     (->> (repeatedly
                                            (fn []
                                              (when-let [k (kv/next i)]
                                                [(mem/copy-to-unpooled-buffer k)
                                                 (mem/copy-to-unpooled-buffer (kv/value i))])))
                                       (take-while identity)
                                       (vec)))))
                   :seek+prevs (with-open [snapshot (kv/new-snapshot kv-store)
                                           i (kv/new-iterator snapshot)]
                                 (when-let [k (kv/seek i (c/->value-buffer k))]
                                   (cons [(mem/copy-to-unpooled-buffer k)
                                          (mem/copy-to-unpooled-buffer (kv/value i))]
                                     (->> (repeatedly
                                            (fn []
                                              (when-let [k (kv/prev i)]
                                                [(mem/copy-to-unpooled-buffer k)
                                                 (mem/copy-to-unpooled-buffer (kv/value i))])))
                                       (take-while identity)
                                       (vec)))))
                   :fsync (kv/fsync kv-store)
                   :delete (kv/delete kv-store [(c/->value-buffer k)])
                   :store (kv/store kv-store
                            [[(c/->value-buffer k)
                              (c/->value-buffer v)]]))))
          (every? true?))))))
