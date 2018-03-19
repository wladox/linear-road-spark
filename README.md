# Linear Road Benchmark on Apache Spark

## Description
This benchmark makes it possible to compare the performance characteristics of SDMS’ relative to each other and to alternative (e.g., Relational Database)
systems. http://www.cs.brandeis.edu/~linearroad/

### start Zookeeper
sudo ~/dev/kafka_2.11-1.0.0/bin/zookeeper-server-start.sh ~/dev/kafka_2.11-1.0.0/config/zookeeper.properties

### start Kafka
sudo ~/dev/kafka_2.11-1.0.0/bin/kafka-server-start.sh ~/dev/kafka_2.11-1.0.0/config/server.properties

### start Prometheus
sudo ~/dev/prometheus-2.1.0.linux-amd64/prometheus --config.file=/home/wladox/dev/prometheus-2.1.0.linux-amd64/prometheus.yml

### start Grafana
sudo service grafana-server start

### start Spark Master
sudo ~/dev/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh

### start Worker
sudo ~/dev/spark-2.2.0-bin-hadoop2.7/sbin/start-slave.sh spark://WladiusPrime:7077

### submit application to Master
sudo ~/dev/spark-2.2.0-bin-hadoop2.7/bin/spark-submit \
--class com.github.wladox.LinearRoadBenchmark \
--master local[*] \
--conf "spark.driver.extraJavaOptions=-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=9010 \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
-Djava.rmi.server.hostname=localhost \
-javaagent://home/wladox/dev/prometheus-2.1.0.linux-amd64/jmx_prometheus_javaagent-0.2.0.jar=8282:/home/wladox/dev/prometheus-2.1.0.linux-amd64/prometheus-config.yml" \
/home/wladox/workspace/LRSparkApplication/target/linear-road-spark-1.0-SNAPSHOT-jar-with-dependencies.jar \
WladiusPrime 7077 /home/wladox/workspace/lrdata/ /home/wladox/workspace/LRSparkApplication/checkpoint/

### generate validator results
sudo time java -cp aerospike-client-3.3.0-jar-with-dependencies.jar:. ValidateMTBQEven3AeroTolls /home/wladox/workspace/LRSparkApplication/data/car.dat 1 /home/wladox/workspace/LRSparkApplication/data/car.dat.tolls.dat 

### start aerospike server
sudo systemctl start aerospike.service