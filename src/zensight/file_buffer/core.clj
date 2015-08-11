(ns zensight.file-buffer.core
  (:import [java.io File OutputStream InputStream FileOutputStream IOException]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file OpenOption StandardOpenOption]))

(defrecord BufferPosition
    ^{:doc "Index of the next byte to be read; count of bytes written"}
    [read-pos write-count])

(def ^:const not-in-range (->BufferPosition -1 -1))

(defprotocol BufferSegment
  "A Buffer (or segment of a Buffer"
  (max-size [segment] "Size or :unlimited")
  (read-bytes [buf-pos segment dst offset len] "Read into dst at offset and len")
  (write-bytes [buf-pos segment src offset len] "Write from src")
  (free-storage [segment closed? buf-pos] "Frees any storage resources"))

(defn readable-bytes
  [buf-pos max-size]
  (let [write-count (.write-count buf-pos)
        end (if (= write-count (.write-count not-in-range))
              max-size
              write-count)]
    (- end (.read-pos buf-pos))))

(defrecord FileSegment
    ^{:doc "BufferSegment backed by a temp file."}
  [fos fc]
  BufferSegment
  (max-size [this] :unlimited)

  (read-bytes
    [this buf-pos dst offset len]
    (let [left (readable-bytes buf-pos len)
          new-len (Math/min len left)
          bb (ByteBuffer/wrap dst offset new-len)
          bb-array (make-array ByteBuffer 1)]
      (aset bb-array 0 bb)
      (.read fc bb-array)))

  (write-bytes
    [this buf-pos src offset len]
    (.write fos src offset len)
    len)

  (free-storage [this closed? buf-pos]
    (when closed?
      (.close fc)
      (.close fos))))

(defn file-segment
  [file]
  (let [fos (FileOutputStream. file)
        opts (make-array OpenOption 2)]
    (aset opts 0 StandardOpenOption/READ)
    ;; Linux impl deletes the file immediately! But we can still
    ;; read/write via open FileChannels and Streams.
    (aset opts 1 StandardOpenOption/DELETE_ON_CLOSE)
    (->FileSegment fos (FileChannel/open (.toPath file) opts))))

(defrecord MemorySegment [buffer buffer-size]
  BufferSegment
  (max-size [this] buffer-size)

  (read-bytes
    [this buf-pos dst offset len]
    (let [left (readable-bytes buf-pos (max-size this))
          new-len (Math/min left len)]
      (System/arraycopy @buffer (.read-pos buf-pos) dst offset new-len)
      new-len))

  (write-bytes
    [this buf-pos src offset len]
    (let [left (- buffer-size (.write-count buf-pos))
          new-len (Math/min left len)]
      (System/arraycopy src offset @buffer (.write-count buf-pos) new-len)
      new-len))

  (free-storage [this closed? buf-pos]
    (reset! buffer nil)))

(defn memory-segment
  [size]
  {:pre [(pos? size)]}
  (->MemorySegment (atom (byte-array size)) size))

(defn relative-buf-pos
  "Given the overall BufferPosition, calculate the relative
  BufferPosition of the given buffer."
  [buf abs-buf-start buf-pos]
  (let [abs-read-pos (.read-pos buf-pos)
        abs-write-count (.write-count buf-pos)
        buf-max-size (max-size buf)
        beyond-end? (fn [i] (if (= :unlimited buf-max-size)
                               false
                               (> i (+ buf-max-size abs-buf-start))))
        rel-read-pos (if (or (beyond-end? abs-read-pos)
                             (< abs-read-pos abs-buf-start))
                       -1 ; not-in-range
                       (- abs-read-pos abs-buf-start))
        rel-write-count (if (or (beyond-end? abs-write-count)
                              (< abs-write-count abs-buf-start))
                        -1 ; not-in-range
                        (- abs-write-count abs-buf-start))]
    (->BufferPosition rel-read-pos rel-write-count)))

(defrecord SegmentedBuffer [buffers]
  BufferSegment
  (max-size [this] :unlimited)

  (read-bytes
    [this buf-pos dst offset len]
    (loop [bufs buffers
           total-read 0        ;; total bytes read so far
           abs-buf-pos buf-pos ;; update the read position in our buf-pos as we go
           abs-buf-start 0     ;; absolute start position of the current buffer
           dst-offset offset   ;; next write position into dst
           dst-len len]        ;; space left in dst
      (if-let [b (first bufs)]
        (let [rel-buf-pos (relative-buf-pos b abs-buf-start abs-buf-pos)]
          (if (not= (.read-pos rel-buf-pos) (.read-pos not-in-range))
            (let [cnt (read-bytes b rel-buf-pos dst dst-offset dst-len)] ; maybe zero, optimizations wanted
              (recur (next bufs)
                     (+ total-read cnt)
                     (->BufferPosition (+ (.read-pos abs-buf-pos) cnt) (.write-count abs-buf-pos))
                     (+ abs-buf-start (if (number? (max-size b))
                                        (max-size b)
                                        0)) ; zero is ok for unlimited, no next buffer
                     (+ dst-offset cnt)
                     (- dst-len cnt)))
            (recur (next bufs)
                   total-read
                   abs-buf-pos
                   (+ abs-buf-start (if (number? (max-size b))
                                        (max-size b)
                                        0))
                   dst-offset
                   dst-len)))
        total-read)))

  (write-bytes
    [this buf-pos src offset len]
    (loop [bufs buffers
           total-written 0     ;; total bytes written so far
           abs-buf-pos buf-pos ;; update write-count in absolute buf-pos as we go
           abs-buf-start 0     ;; absolute start position of the current buffer
           src-offset offset   ;; next read position in src
           src-len len]        ;; space left in src
      (if-let [b (first bufs)]
        (let [rel-buf-pos (relative-buf-pos b abs-buf-start abs-buf-pos)]
          (if (not= (.write-count rel-buf-pos) (.write-count not-in-range))
            (let [cnt (write-bytes b rel-buf-pos src src-offset src-len)] ; maybe zero, optimizations wanted
              (recur (next bufs)
                     (+ total-written cnt)
                     (->BufferPosition (.read-pos abs-buf-pos) (+ (.write-count abs-buf-pos) cnt))
                     (+ abs-buf-start (if (number? (max-size b))
                                        (max-size b)
                                        0)) ; zero is ok for unlimited, no next buffer
                     (+ src-offset cnt)
                     (- src-len cnt)))
            (recur (next bufs)
                   total-written
                   abs-buf-pos
                   (+ abs-buf-start (if (number? (max-size b))
                                        (max-size b)
                                        0))
                   src-offset
                   src-len)))
        total-written)))

  (free-storage [this closed? buf-pos]
    (if closed?
      (doseq [b buffers]
        (free-storage b true nil))
      (loop [bufs buffers
             abs-buf-end 0]
        (when-let [b (first bufs)]
          (let [sz (if (number? (max-size b))
                     (max-size b)
                     0)
                abs-buf-end (+ abs-buf-end sz)]
            (when (> (.read-pos buf-pos) abs-buf-end)
              (free-storage b false buf-pos)
              (recur (next bufs) abs-buf-end))))))))

(defn segmented-buffer
  "Creates a SegmentedBuffer with initial MemorySegments and final
  FileSegment"
  ([file]
   (segmented-buffer (* 5 1024 1024) (* 1024 1024))) ;; 5MB in 1MB segments
  ([file threshold max-buf-size]
   {:pre [(>= threshold 0)]}
   (if (zero? threshold)
     (->SegmentedBuffer [(file-segment file)])
     (let [buf-count (Math/floor (/ threshold max-buf-size))
           remainder (- threshold (* buf-count max-buf-size))
           mem-segments (map #(memory-segment %)
                             (concat (repeat buf-count max-buf-size)
                                     (when (> remainder 0)
                                       (vector remainder))))
           all-segments (concat mem-segments (vector (file-segment file)))]
       (->SegmentedBuffer all-segments)))))

(defn maybe-wait-for-bytes
  [buf-pos closed? lock-obj]
  (locking lock-obj
    (when (and (not (:fbos-closed? @closed?))
               (= (.read-pos buf-pos)
                  (.write-count buf-pos)))
      (.wait lock-obj))))

;; Class FBOutputStream ------------------------------------------------
(gen-class
 :name zensight.file-buffer.core.FBOutputStream
 :extends java.io.OutputStream
 :state state
 :init init
 :constructors {[Object zensight.file_buffer.core.BufferSegment clojure.lang.Atom clojure.lang.Atom] []}
 :methods [[getTotalBytes [] Long]
           [isClosed [] Boolean]]
 :prefix fbos-)

(defn fbos-init
  [lock-obj buffer buf-pos closed?]
  [[] {:lock-obj lock-obj
       :buffer buffer
       :buf-pos buf-pos
       :closed? closed?}])

(defn fbos-getTotalBytes
  [this]
  (-> (.state this) :buf-pos deref .write-count))

(defn fbos-isClosed
  [this]
  (-> (.state this)
      :closed?
      deref
      :fbos-closed?))

(defn fbos-close
  [this]
  (let [state (.state this)
        lock-obj (:lock-obj state)]

    (locking lock-obj
      (swap! (:closed? state) assoc-in [:fbos-closed?] true)
      ;; unblock any reader threads
      (.notifyAll lock-obj))

    ;; free all storage when both streams are closed
      (when (every? true? (vals @(:closed? state)))
        (free-storage (:buffer state) true nil))))

;; uphold Outputstream contract
(defn fbos-write
  ([this arg1]
   (if (number? arg1) ; write a single byte
     (let [src (byte-array 1)]
       (aset-byte src 0 arg1)
       (fbos-write this src 0 1))
     (fbos-write this arg1 0 (count arg1)))) ; write an array of bytes
  ([this src offset len]
   (let [state (.state this)]
     (if (:fbos-closed? @(:closed? state))
       (throw (IOException. "stream closed"))
       (let [buffer (:buffer state)
             buf-pos (:buf-pos state)
             lock-obj (:lock-obj state)
             cnt (write-bytes buffer @buf-pos src offset len)]
         (swap! buf-pos update-in [:write-count] + cnt)
         (locking lock-obj
           (.notifyAll lock-obj))))
     nil)))

;; Class FBInputStream -------------------------------------------------
(gen-class
 :name zensight.file-buffer.core.FBInputStream
 :extends java.io.InputStream
 :state state
 :init init
 :constructors {[Object zensight.file_buffer.core.BufferSegment clojure.lang.Atom clojure.lang.Atom] []}
 :prefix fbis-)

(defn fbis-init
  [lock-obj buffer buf-pos closed?]
  [[] {:lock-obj lock-obj
       :buffer buffer
       :buf-pos buf-pos
       :closed? closed?}])

;; uphold InputStream contract
(defn fbis-read
  ([this]
   (let [dst (byte-array 1)
         cnt (fbis-read this dst 0 1)]
     (if (= cnt 1)
       (aget dst 0)
       cnt)))
  ([this dst]
   (fbis-read this dst 0 (count dst)))
  ([this dst offset len]
   (let [state (.state this)
         closed-and-fully-read? (fn [state]
                                  (let [buf-pos @(:buf-pos state)]
                                    (and (:fbos-closed? @(:closed? state))
                                         (= (.read-pos buf-pos)
                                            (.write-count buf-pos)))))]
     (cond
       (zero? (count dst))
       0

       (closed-and-fully-read? state)
       -1 ; EOF

       :else
       (let [buffer (:buffer state)
             buf-pos (:buf-pos state)
             lock-obj (:lock-obj state)]
         (maybe-wait-for-bytes @buf-pos (:closed? state) lock-obj)

         (if (closed-and-fully-read? state)
           -1 ; EOF, writer may have closed (with no new bytes) while waiting
           (let [cnt (read-bytes buffer @buf-pos dst offset len)
                 both-closed? (every? true? (vals @(:closed? state)))]
             (swap! buf-pos update-in [:read-pos] + cnt)
             (free-storage buffer both-closed? @buf-pos)
             cnt)))))))

(defn fbis-close [this]
  (let [state (.state this)]
    (swap! (:closed? state) assoc-in [:fbis-closed?] true)
    ;; free all storage when both streams are closed
    (when (every? true? (vals @(:closed? state)))
      (free-storage (:buffer state) true nil))))

(defn fbis-available
  [this]
  (let [buf-pos @(:buf-pos (.state this))
        avail (- (.write-count buf-pos)
                 (.read-pos buf-pos))]
    (Math/max avail 0)))

(defn fbis-markSupported [this]
  false)

(defn fbis-mark [this readlimit]
  nil) ; no-op

(defn fbis-reset [this] (throw (IOException. "Mark not suported")))

;; Class FileBuffer ----------------------------------------------------
(gen-class
 :name zensight.file-buffer.core.FileBuffer
 :state state
 :init init
 :constructors {[Long Long] []}
 :methods [[getThreshold [] Long] ;; stuff not in interfaces or superclass
           [getOutputStream [] java.io.OutputStream]
           [getInputStream [] java.io.InputStream]
           [isClosed [] Boolean]
           [getTotalBytes [] Long]] ; testing only
 :prefix fb-)

(defn fb-init [threshold max-buf-size]
  (let [lock-obj (Object.)
        closed? (atom {:fbos-closed? false
                       :fbis-closed? false})
        file (File/createTempFile "FileBackedBuffer" nil)
        buf-pos (atom (->BufferPosition 0 0))
        buffer (segmented-buffer file threshold max-buf-size)
        os (zensight.file-buffer.core.FBOutputStream. lock-obj buffer buf-pos closed?)
        is (zensight.file-buffer.core.FBInputStream. lock-obj buffer buf-pos closed?)]
    [[] ; args to super class
     {:threshold threshold
      :file file
      :buffer buffer
      :buf-pos buf-pos
      :os os
      :is is
      :closed? closed?}]))

(defn fb-getThreshold
  [this]
  (:threshold (.state this)))

(defn fb-getOutputStream
  [this]
  (:os (.state this)))

(defn fb-getInputStream
  [this]
  (:is (.state this)))

(defn fb-getTotalBytes
  [this]
  (-> (.state this)
      :buf-pos
      deref
      .write-count))

(defn fb-isClosed
  [this]
  (->> (.state this)
       :closed?
       deref
       vals
       (every? true?)))
