(ns zensight.file-buffer.core-test
  (:require [zensight.file-buffer.core :as fb]
            [clojure.stacktrace :refer [print-stack-trace]])
  (:import [zensight.file-buffer.core FileBuffer FBOutputStream FBInputStream]
           [java.io File])
  (:use [midje.sweet]))

(facts "Relative positions are usable for read and write"
      (fact "Correct starting position"
            (fb/relative-buf-pos (fb/->MemorySegment (byte-array 3) 3)
                                 0
                                 (fb/->BufferPosition 0 0)) => (fb/->BufferPosition 0 0))

      (fact "Both positions inside first buffer"
            (fb/relative-buf-pos (fb/->MemorySegment (byte-array 3) 3)
                                 0
                                 (fb/->BufferPosition 1 1)) => (fb/->BufferPosition 1 1))

      (fact "Postions before given buffer"
            (fb/relative-buf-pos (fb/->MemorySegment (byte-array 3) 3)
                                 4
                                 (fb/->BufferPosition 0 0)) => (fb/->BufferPosition -1 -1))

      (fact "Postions before given buffer"
            (fb/relative-buf-pos (fb/->MemorySegment (byte-array 3) 3)
                                 4
                                 (fb/->BufferPosition 8 8)) => (fb/->BufferPosition -1 -1))

      (fact "Read position in buffer, write in next buffer"
            (fb/relative-buf-pos (fb/->MemorySegment (byte-array 3) 3)
                                 4
                                 (fb/->BufferPosition 6 8)) => (fb/->BufferPosition 2 -1))

      (fact "Write position in buffer, pos in previous buffer"
            (fb/relative-buf-pos (fb/->MemorySegment (byte-array 3) 3)
                                 4
                                 (fb/->BufferPosition 2 6)) => (fb/->BufferPosition -1 2)))

(fact "Can read and write a single byte"
      (let [fb (FileBuffer. 1024 256)
            os (.getOutputStream fb)]
        (with-open [is (.getInputStream fb)
                    os os]
          (.write os 65) ; "A"
          (.read is) => 65)))

(fact "Can read and write from byte arrays"
      (let [fb (FileBuffer. 1024 256)
            os (.getOutputStream fb)
            buf (byte-array 3)]
        (with-open [is (.getInputStream fb)
                    os os]
          (.write os (.getBytes "abc"))
          (.read is buf) => 3
          (aget buf 0) => 97
          (aget buf 1) => 98
          (aget buf 2) => 99)))

(fact "Can read and write from byte arrays with offsets"
      (let [fb (FileBuffer. 1024 256)
            os (.getOutputStream fb)
            buf (byte-array 10)]
        (with-open [is (.getInputStream fb)
                    os os]
          (aset-byte buf 0 65)
          (aset-byte buf 1 65)
          (.write os (.getBytes "abc"))
          (.read is buf 2 3) => 3
          (aget buf 2) => 97
          (aget buf 3) => 98
          (aget buf 4) => 99)))

(fact "Reading blocks waiting for writing"
      (let [fb (FileBuffer. 1024 256)
            os (.getOutputStream fb)]
        (with-open [is (.getInputStream fb)
                    os os]
          (future (Thread/sleep 250)
                  (.write os 65))
          (.read is) => 65)))

(fact "Read of a closed buffer gives -1"
      (let [fb (FileBuffer. 1024 256)
            os (.getOutputStream fb)
            is (.getInputStream fb)]
        (.write os 65)
        (.read is) => 65
        (.close os)
        (.read is) => -1
        (.close is)))

(fact "Reads can span memory segments"
      (let [fb (FileBuffer. 26 13)
            os (.getOutputStream fb)
            buf (byte-array 26)]
        (with-open [is (.getInputStream fb)
                    os os]
          (.write os (.getBytes "abcdefghijklmnopqrstuvwxy"))
          (.read is buf) => 25
          (.substring (String. buf) 0 25) => "abcdefghijklmnopqrstuvwxy")))

(fact "Reads can span from memory to file segments"
      (let [fb (FileBuffer. 13 13)
            os (.getOutputStream fb)
            buf (byte-array 26)]
        (with-open [is (.getInputStream fb)
                    os os]
          (.write os (.getBytes "abcdefghijklmnopqrstuvwxy")) ; 25 chars
          (.read is buf) => 25
          (.substring (String. buf) 0 25) => "abcdefghijklmnopqrstuvwxy")))


(defn build-xml [items]
  (str "<batchInfo>"
       (reduce str
               (interpose "\n"
                          (map #(str "<records><name>n" % "</name><email>e" % "</email><records>")
                               (range 0 items))))
       "</batchInfo>"))

(fact "Readers pull at speed of writer - 3-arg read"
      (let [xml (build-xml 100)
            xml-byte-cnt (count xml)
            xml-bytes (.getBytes xml)
            fb (FileBuffer. 1024 128) ; spills to disk
            buf (byte-array xml-byte-cnt)]

        ;; write on another thread, slowly
        (future
          (with-open [os (.getOutputStream fb)]
            (loop [written 0]
              (let [left (- xml-byte-cnt written)
                    to-write (Math/min 100 left)]
                (when (pos? left)
                  (Thread/sleep 100)
                  (.write os xml-bytes written to-write)
                  (recur (+ written to-write)))))))

        ;; read into buf until empty
        (with-open [is (.getInputStream fb)]
          (loop [read-bytes 0]
            (let [offset read-bytes
                  buf-avail (- (count buf) read-bytes)
                  cnt (.read is buf offset buf-avail)]
              (when (not (= -1 cnt))
                (recur (+ read-bytes cnt))))))
        (.substring (String. buf) 0 xml-byte-cnt) => xml))

(fact "Readers pull at speed of writer - 1-arg buf read"
      (let [xml (build-xml 100)
            xml-byte-cnt (count xml)
            xml-bytes (.getBytes xml)
            fb (FileBuffer. 1024 128)
            big-buf (byte-array xml-byte-cnt)] ; spills to disk

        ;; write on another thread, slowly
        (future
          (with-open [os (.getOutputStream fb)]
            (loop [written 0]
              (let [left (- xml-byte-cnt written)
                    to-write (Math/min 100 left)]
                (when (pos? left)
                  (Thread/sleep 100)
                  (.write os xml-bytes written to-write)
                  (recur (+ written to-write)))))))

        ;; read into buf until empty
        (with-open [is (.getInputStream fb)]
          (let [total-read-bytes
                (loop [read-bytes 0]
                  (let [buf (byte-array 100)
                        cnt (.read is buf)]
                    (if (not (= -1 cnt))
                      (do
                        (System/arraycopy buf 0 big-buf read-bytes cnt)
                        (recur (+ read-bytes cnt)))
                      (+ read-bytes))))]
            total-read-bytes => xml-byte-cnt
            (String. big-buf) => xml))))

(fact "Readers pull at speed of writer - 1-arg byte read"
      (let [xml (build-xml 100)
            xml-byte-cnt (count xml)
            xml-bytes (.getBytes xml)
            fb (FileBuffer. 1024 128)
            big-buf (byte-array xml-byte-cnt)] ; spills to disk

        ;; write on another thread, slowly
        (future
          (with-open [os (.getOutputStream fb)]
            (loop [written 0]
              (let [left (- xml-byte-cnt written)
                    to-write (Math/min 100 left)]
                (when (pos? left)
                  (Thread/sleep 100)
                  (.write os xml-bytes written to-write)
                  (recur (+ written to-write)))))))

        ;; read into buf until empty
        (with-open [is (.getInputStream fb)]
          (let [total-read-bytes
                (loop [read-bytes 0]
                  (let [byte (.read is)]
                    (if (not (= -1 byte))
                      (do
                        (aset-byte big-buf read-bytes byte)
                        (recur (inc read-bytes)))
                      (+ read-bytes))))]
            total-read-bytes => xml-byte-cnt
            (String. big-buf) => xml))))

(fact "Close of writer before reader done doesn't prematurely end reading"
      (let [xml (build-xml 100)
            xml-byte-cnt (count xml)
            xml-bytes (.getBytes xml)
            fb (FileBuffer. 1024 128)
            big-buf (byte-array xml-byte-cnt)] ; spills to disk

        ;; write on another thread, slowly
        (future
          (with-open [os (.getOutputStream fb)]
            (loop [written 0]
              (let [left (- xml-byte-cnt written)
                    to-write (Math/min 100 left)]
                (when (pos? left)
                  (Thread/sleep 1)
                  (.write os xml-bytes written to-write)
                  (recur (+ written to-write))))))
          (.getTotalBytes fb) => xml-byte-cnt)

        ;; read into buf until empty
        (with-open [is (.getInputStream fb)]
          (let [total-read-bytes
                (loop [read-bytes 0]
                  (let [buf (byte-array 100)
                        cnt (.read is buf)]
                    (if (not (= -1 cnt))
                      (do
                        (System/arraycopy buf 0 big-buf read-bytes cnt)
                        (Thread/sleep 10)
                        (recur (+ read-bytes cnt)))
                      (+ read-bytes))))]
            total-read-bytes => xml-byte-cnt
            (String. big-buf) => xml))))

(fact "Quick close of outputstream doesn't prematurely end reader"
      (let [data "abcdef"
            data-byte-cnt (count data)
            data-bytes (.getBytes data)
            fb (FileBuffer. 4 2)
            big-buf (byte-array data-byte-cnt)] ; spills to disk
        (with-open [os (.getOutputStream fb)]
          (.write os data-bytes))

        (with-open [is (.getInputStream fb)]
          (.read is big-buf) => 6
          (.read is) => -1)))

(fact "Close of writer when fully read releases reader"
      (let [data "abcdef"
            data-byte-cnt (count data)
            data-bytes (.getBytes data)
            fb (FileBuffer. 4 2)
            os (.getOutputStream fb)
            is (.getInputStream fb)
            big-buf-size (+ 1 data-byte-cnt)
            big-buf (byte-array big-buf-size)] ; spills to disk

        ;; write the 'all' the data
        (.write os data-bytes)

        (let [f-v (future
                    (with-open [stream is]
                      ;; will read all bytes that will every be written
                      (.read stream big-buf 0 big-buf-size) => data-byte-cnt
                      ;; will block until the writer closes his stream, gets EOF
                      (.read stream big-buf 0 big-buf-size)))]
          (Thread/sleep 1000)
          (.close os)
          @f-v => -1)))
