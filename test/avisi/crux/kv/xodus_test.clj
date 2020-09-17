(ns avisi.crux.kv.xodus-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [crux.codec :as c]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [crux.io :as cio]
            avisi.crux.kv.xodus
            crux.config
            [crux.topology :as topo])
  (:import [org.agrona.concurrent UnsafeBuffer]
           [java.nio ByteOrder]))

(def ^:dynamic *kv*)
(def ^:dynamic *kv-module* 'avisi.crux.kv.xodus/kv)
(def ^:dynamic *kv-opts* {})

(defn with-kv-store [f]
  (let [db-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [kv (topo/start-component *kv-module* nil (assoc *kv-opts* ::kv/db-dir (str db-dir)))]
        (binding [*kv* kv]
          (f)))
      (finally
        (cio/delete-dir db-dir)))))

(t/use-fixtures :each with-kv-store)

(declare value seek seek-and-iterate long->bytes bytes->long compare-bytes bytes=?)
(t/deftest test-store-and-value []
                                (t/testing "store, retrieve and seek value"
                                  (kv/store *kv* [[(long->bytes 1) (.getBytes "Crux")]])
                                  (t/is (= "Crux" (String. ^bytes (value *kv* (long->bytes 1)))))
                                  (t/is (= [1 "Crux"] (let [[k v] (seek *kv* (long->bytes 1))]
                                                        [(bytes->long k) (String. ^bytes v)]))))

                                (t/testing "non existing key"
                                  (t/is (nil? (value *kv* (long->bytes 2))))))

(t/deftest test-can-store-and-delete-all-116 []
                                             (let [number-of-entries 500]
                                               (kv/store *kv* (map (fn [i]
                                                                     [(long->bytes i) (long->bytes (inc i))])
                                                                (range number-of-entries)))
                                               (doseq [i (range number-of-entries)]
                                                 (t/is (= (inc i) (bytes->long (value *kv* (long->bytes i))))))

                                               (t/testing "deleting all keys in random order, including non existent keys"
                                                 (kv/delete *kv* (for [i (shuffle (range (long (* number-of-entries 1.2))))]
                                                                   (long->bytes i)))
                                                 (doseq [i (range number-of-entries)]
                                                   (t/is (nil? (value *kv* (long->bytes i))))))))

(t/deftest test-seek-and-iterate-range []
                                       (doseq [[^String k v] {"a" 1 "b" 2 "c" 3 "d" 4}]
                                         (kv/store *kv* [[(.getBytes k) (long->bytes v)]]))

                                       (t/testing "seek range is exclusive"
                                         (t/is (= [["b" 2] ["c" 3]]
                                                 (for [[^bytes k v] (seek-and-iterate *kv*
                                                                      #(neg? (compare-bytes % (.getBytes "d")))
                                                                      (.getBytes "b"))]
                                                   [(String. k) (bytes->long v)]))))

                                       (t/testing "seek range after existing keys returns empty"
                                         (t/is (= [] (seek-and-iterate *kv* #(neg? (compare-bytes % (.getBytes "d"))) (.getBytes "d"))))
                                         (t/is (= [] (seek-and-iterate *kv* #(neg? (compare-bytes % (.getBytes "f")%)) (.getBytes "e")))))

                                       (t/testing "seek range before existing keys returns keys at start"
                                         (t/is (= [["a" 1]] (for [[^bytes k v] (into [] (seek-and-iterate *kv* #(neg? (compare-bytes % (.getBytes "b"))) (.getBytes "0")))]
                                                              [(String. k) (bytes->long v)])))))

(t/deftest test-seek-between-keys []
                                  (doseq [[^String k v] {"a" 1 "c" 3}]
                                    (kv/store *kv* [[(.getBytes k) (long->bytes v)]]))

                                  (t/testing "seek returns next valid key"
                                    (t/is (= ["c" 3]
                                            (let [[^bytes k v] (seek *kv* (.getBytes "b"))]
                                              [(String. k) (bytes->long v)])))))

(t/deftest test-seek-and-iterate-prefix []
                                        (doseq [[^String k v] {"aa" 1 "b" 2 "bb" 3 "bcc" 4 "bd" 5 "dd" 6}]
                                          (kv/store *kv* [[(.getBytes k) (long->bytes v)]]))

                                        (t/testing "seek within bounded prefix returns all matching keys"
                                          (t/is (= [["b" 2] ["bb" 3] ["bcc" 4] ["bd" 5]]
                                                  (for [[^bytes k v] (into [] (seek-and-iterate *kv* #(bytes=? (.getBytes "b") % (alength (.getBytes "b"))) (.getBytes "b")))]
                                                    [(String. k) (bytes->long v)]))))

                                        (t/testing "seek within bounded prefix before or after existing keys returns empty"
                                          (t/is (= [] (into [] (seek-and-iterate *kv* (partial bytes=? (.getBytes "0")) (.getBytes "0")))))
                                          (t/is (= [] (into [] (seek-and-iterate *kv* (partial bytes=? (.getBytes "e")) (.getBytes "0")))))))

(t/deftest test-delete-keys []
                            (t/testing "store, retrieve and delete value"
                              (kv/store *kv* [[(long->bytes 1) (.getBytes "Crux")]])
                              (t/is (= "Crux" (String. ^bytes (value *kv* (long->bytes 1)))))
                              (kv/delete *kv* [(long->bytes 1)])
                              (t/is (nil? (value *kv* (long->bytes 1))))
                              (t/testing "deleting non existing key is noop"
                                (kv/delete *kv* [(long->bytes 1)]))))

(t/deftest test-compact []
                        (t/testing "store, retrieve and delete value"
                          (kv/store *kv* [[(.getBytes "key-with-a-long-prefix-1") (.getBytes "Crux")]])
                          (kv/store *kv* [[(.getBytes "key-with-a-long-prefix-2") (.getBytes "is")]])
                          (kv/store *kv* [[(.getBytes "key-with-a-long-prefix-3") (.getBytes "awesome")]])
                          (t/testing "compacting"
                            (kv/compact *kv*))
                          (t/is (= "Crux" (String. ^bytes (value *kv* (.getBytes "key-with-a-long-prefix-1")))))
                          (t/is (= "is" (String. ^bytes (value *kv* (.getBytes "key-with-a-long-prefix-2")))))
                          (t/is (= "awesome" (String. ^bytes (value *kv* (.getBytes "key-with-a-long-prefix-3")))))))

(t/deftest test-sanity-check-can-start-with-sync-enabled
  (binding [*kv-opts* {:crux.kv/sync? true}]
    (with-kv-store
      (fn []
        (kv/store *kv* [[(long->bytes 1) (.getBytes "Crux")]])
        (t/is (= "Crux" (String. ^bytes (value *kv* (long->bytes 1)))))))))

(t/deftest test-sanity-check-can-fsync
  (kv/store *kv* [[(long->bytes 1) (.getBytes "Crux")]])
  (kv/fsync *kv*)
  (t/is (= "Crux" (String. ^bytes (value *kv* (long->bytes 1))))))

(t/deftest test-can-get-from-snapshot
  (kv/store *kv* [[(long->bytes 1) (.getBytes "Crux")]])
  (with-open [snapshot (kv/new-snapshot *kv*)]
    (t/is (= "Crux" (String. (mem/->on-heap (kv/get-value snapshot (long->bytes 1))))))
    (t/is (nil? (kv/get-value snapshot (long->bytes 2))))))

(t/deftest test-can-read-write-concurrently
  (let [w-fs (for [_ (range 128)]
               (future
                 (kv/store *kv* [[(long->bytes 1) (.getBytes "Crux")]])))]
    @(first w-fs)
    (let [r-fs (for [_ (range 128)]
                 (future
                   (String. ^bytes (value *kv* (long->bytes 1)))))]
      (mapv deref w-fs)
      (doseq [r-f r-fs]
        (t/is (= "Crux" @r-f))))))

(t/deftest test-prev-and-next []
                              (doseq [[^String k v] {"a" 1 "c" 3}]
                                (kv/store *kv* [[(.getBytes k) (long->bytes v)]]))

                              (with-open [snapshot (kv/new-snapshot *kv*)
                                          i (kv/new-iterator snapshot)]
                                (t/testing "seek returns next valid key"
                                  (let [k (kv/seek i (mem/as-buffer (.getBytes "b")))]
                                    (t/is (= ["c" 3] [(String. (mem/->on-heap k)) (bytes->long (mem/->on-heap (kv/value i)))]))))
                                (t/testing "prev, iterators aren't bidirectional"
                                  (t/is (= "a" (String. (mem/->on-heap (kv/prev i)))))
                                  (t/is (nil? (kv/prev i))))))

(tcct/defspec test-basic-generative-store-and-get-value 20
  (prop/for-all [kvs (gen/not-empty (gen/map
                                      gen/simple-type-printable
                                      gen/int {:num-elements 100}))]
    (let [kvs (->> (for [[k v] kvs]
                     [(c/->value-buffer k)
                      (c/->value-buffer v)])
                (into {}))]
      (with-kv-store
        (fn []
          (kv/store *kv* kvs)
          (with-open [snapshot (kv/new-snapshot *kv*)]
            (->> (for [[k v] kvs]
                   (= v (kv/get-value snapshot k)))
              (every? true?))))))))

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
    (with-kv-store
      (fn []
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
                     :get-value (with-open [snapshot (kv/new-snapshot *kv*)]
                                  (kv/get-value snapshot (c/->value-buffer k)))
                     :seek (with-open [snapshot (kv/new-snapshot *kv*)
                                       i (kv/new-iterator snapshot)]
                             (kv/seek i (c/->value-buffer k)))
                     :seek+value (with-open [snapshot (kv/new-snapshot *kv*)
                                             i (kv/new-iterator snapshot)]
                                   (when (kv/seek i (c/->value-buffer k))
                                     (kv/value i)))
                     :seek+nexts (with-open [snapshot (kv/new-snapshot *kv*)
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
                     :seek+prevs (with-open [snapshot (kv/new-snapshot *kv*)
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
                     :fsync (kv/fsync *kv*)
                     :delete (kv/delete *kv* [(c/->value-buffer k)])
                     :store (kv/store *kv*
                              [[(c/->value-buffer k)
                                (c/->value-buffer v)]]))))
            (every? true?)))))))

;; TODO: These helpers convert back and forth to bytes, would be good
;; to get rid of this, but that requires changing removing the byte
;; arrays above in the tests. The tested code uses buffers internally.

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

(t/deftest test-performance-off-heap
  (if (and (Boolean/parseBoolean (System/getenv "CRUX_KV_PERFORMANCE"))
        (if-let [backend (System/getenv "CRUX_KV_PERFORMANCE_BACKEND")]
          (= backend (str *kv-module*))
          true))
    (let [n 1000000
          ks (vec (for [n (range n)]
                    (mem/->off-heap (.getBytes (format "%020x" n)))))]
      (println *kv-module* "off-heap")
      (t/is (= n (count ks)))
      (t/is (mem/off-heap? (first ks)))

      (System/gc)
      (println "Writing")
      (time
        (kv/store *kv* (for [k ks]
                         [k k])))

      (System/gc)
      (println "Reading")
      (time
        (do (dotimes [_ 10]
              (time
                ;; TODO: Note, the cached decorator still uses
                ;; bytes, so we grab the underlying kv store.
                (with-open [snapshot (kv/new-snapshot (:kv *kv*))
                            i (kv/new-iterator snapshot)]
                  (dotimes [idx n]
                    (let [idx (- (dec n) idx)
                          k (get ks idx)]
                      (assert (mem/buffers=? k (kv/seek i k)))
                      (assert (mem/buffers=? k (kv/value i))))))))
            (println "Done")))
      (println))
    (t/is true)))
