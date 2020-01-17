#!/bin/bash -e

/usr/sbin/sshd
hdfs namenode -format
start-dfs.sh
start-yarn.sh

## This folder is used for logging spark-events
mkdir -p /tmp/spark-events

# Wait for 10s to let all the hadoop processes to start up.
sleep 10

echo "** [NOTE] Started hadoop services - please start the hive metastore for spark-submit (hive --service metastore &)."
if [[ -e "/FlashML/project" ]]
then
	echo "** [NOTE] External folder has been mounted to /FlashML/project"
fi
# Copy over the FlashML jar to HDFS
# Find the name of main jar
mainJar=`ls flashml*[^tests].jar`
hadoop fs -mkdir /flashml
hadoop fs -put $mainJar /flashml
echo "** [NOTE] FlashML jar ($mainJar) is available at hdfs:///flashml/$mainJar"
echo "** [NOTE] A sample spark-submit command would be: spark-submit --master local[2] --conf spark.yarn.maxAppAttempts=1 --class com.tfs.flashml.FlashML hdfs:///flashml/$mainJar config.json"
echo "** [NOTE] Dropping to bash shell."

/bin/bash