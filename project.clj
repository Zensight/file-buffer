(defproject file-buffer "0.1.0"
  :description "A buffer for large data volumes backed by a file."
  :url "https://github.com/Zensight/file-buffer"
  :license {:name "The MIT License (MIT)"
            :url "http://opensource.org/licenses/mit-license.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :aot [zensight.file-buffer.core]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]
                   :plugins      [[lein-midje "3.1.3"]]}})
