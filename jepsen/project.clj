(defproject jepsen.etcd "0.1.0-SNAPSHOT"
  :description "A Jepsen test for Nuft"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.nuft
  :jvm-opts ["-Dcom.sun.management.jmxremote"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.9-SNAPSHOT"]])
