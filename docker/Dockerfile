FROM praas/spark-on-hadoop-hive:sp2.4.1
MAINTAINER 247ilabs<dsg_dsi@247-inc.com>

USER root

RUN useradd -ms /bin/bash hdfs

RUN mkdir /home/hdfs/namenode /home/hdfs/datanode\
  && chmod 777 /home/hdfs/datanode  \
  && chmod 777 /home/hdfs/namenode \
  && chown hdfs:hdfs /home/hdfs/datanode \
  && chown hdfs:hdfs /home/hdfs/namenode

#RUN useradd -ms /bin/bash praas
RUN mkdir /FlashML

COPY run-flashml.sh /FlashML/run-flashml.sh
COPY run-flashml-tests.sh /FlashML/run-flashml-tests.sh
RUN chmod 777 /FlashML/run-flashml.sh
RUN chmod 777 /FlashML/run-flashml-tests.sh

COPY FlashML /FlashML
COPY flashml-*.jar /FlashML/
COPY scalatest_2.11-3.0.5.jar /FlashML/scalatest_2.11-3.0.5.jar
COPY scalactic_2.11-3.0.5.jar /FlashML/scalactic_2.11-3.0.5.jar

WORKDIR /FlashML