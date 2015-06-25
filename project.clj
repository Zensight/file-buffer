(defproject co.zensight/file-buffer "0.1.2-SNAPSHOT"
  :description "A buffer for large data volumes backed by a file."
  :url "https://github.com/Zensight/file-buffer"
  :license {:name "The MIT License (MIT)"
            :url "http://opensource.org/licenses/mit-license.html"}
  :scm {:name "git"
        :url "https://github.com/Zensight/file-buffer"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :aot [zensight.file-buffer.core]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]
                   :plugins      [[lein-midje "3.1.3"]]}})
