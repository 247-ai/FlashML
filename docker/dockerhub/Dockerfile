FROM centos:latest
MAINTAINER 247ilabs<dsg_dsi@247-inc.com>

USER root

# Install dev tools
RUN yum clean all; \
    rpm --rebuilddb; \
    yum install -y initscripts curl which tar sudo rsync openssh-server openssh-clients

# SSH setting
COPY config/ssh-config /root/.ssh/config
RUN chmod 600 /root/.ssh/config
RUN chown root:root /root/.ssh/config

# Generate all the key types
# This is to avoid getting all the "Could not load host key" errors when sshd service (re-)starts.
RUN /usr/bin/ssh-keygen -A
# Now set up key for root account
RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
# Finally set up to run sshd automatically
RUN echo "/usr/sbin/sshd" >> ~/.bashrc


#############################################################################
# Install java
RUN yum install -y java-1.8.0-openjdk
ENV JAVA_HOME=/etc/alternatives/jre

#############################################################################
# Install Scala
ARG SCALA_VERSION=2.11.12
ENV SCALA_HOME=/usr/local/scala-$SCALA_VERSION
RUN mkdir -p "${SCALA_HOME}" \
  && export SCALA_ARCHIVE=scala-$SCALA_VERSION.tgz \
  && export SCALA_DOWNLOAD_PATH=scala/$SCALA_VERSION/$SCALA_ARCHIVE \
  && curl -#SL https://downloads.lightbend.com/$SCALA_DOWNLOAD_PATH | \
    tar -xz -C $SCALA_HOME --strip-components 1 \
  && rm -rf $SCALA_ARCHIVE

ENV PATH=$PATH:$SCALA_HOME/bin

#############################################################################
# Install Spark
ARG SPARK_VERSION=2.4.1
ENV SPARK_HOME=/usr/local/spark-$SPARK_VERSION
RUN mkdir -p "${SPARK_HOME}" \
  && export SPARK_ARCHIVE=spark-$SPARK_VERSION-bin-hadoop2.7.tgz \
  && export SPARK_DOWNLOAD_PATH=/spark/spark-$SPARK_VERSION/$SPARK_ARCHIVE \
  && curl -#SL http://mirrors.estointernet.in/apache/$SPARK_DOWNLOAD_PATH | \
    tar -xz -C $SPARK_HOME --strip-components 1 \
  && rm -rf $SPARK_ARCHIVE

COPY spark-env.sh $SPARK_HOME/conf/spark-env.sh
COPY spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
ENV PATH=$PATH:$SPARK_HOME/bin

# Ports
EXPOSE 6066 7077 8080 8081 4040 4041 18080

# Fix environment for other users
RUN echo "export SPARK_HOME=$SPARK_HOME" >> /etc/bash.bashrc \
  && echo 'export PATH=$PATH:$SPARK_HOME/bin'>> /etc/bash.bashrc

#############################################################################
# Install Hive
ARG HIVE_VERSION=1.2.2
ENV HIVE_HOME=/usr/local/hive-$HIVE_VERSION
RUN mkdir -p "${HIVE_HOME}" \
  && export HIVE_ARCHIVE=apache-hive-$HIVE_VERSION-bin.tar.gz \
  && export HIVE_DOWNLOAD_PATH=hive/hive-$HIVE_VERSION/$HIVE_ARCHIVE \
  && curl -#SL http://mirrors.estointernet.in/apache/$HIVE_DOWNLOAD_PATH | \
    tar -xz -C $HIVE_HOME --strip-components 1 \
  && rm -rf $HIVE_ARCHIVE

ENV PATH=$PATH:$HIVE_HOME/bin

#############################################################################
# Install hadoop
ARG HADOOP_VERSION=2.7.7

RUN curl -#SL http://mirrors.estointernet.in/apache/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz | tar -xz -C /usr/local/ \
  && cd /usr/local && ln -s ./hadoop-$HADOOP_VERSION hadoop \
  && cd /usr/local/hadoop && mkdir -p logs \
  && rm -rf hadoop-$HADOOP_VERSION.tar.gz

# Set up necessary environment variables
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_PREFIX=/usr/local/hadoop
ENV HADOOP_COMMON_HOME=/usr/local/hadoop
ENV HADOOP_HDFS_HOME=/usr/local/hadoop
ENV HADOOP_MAPRED_HOME=/usr/local/hadoop
ENV HADOOP_YARN_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV YARN_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
ENV PATH=$PATH:$HADOOP_PREFIX/bin:$HADOOP_PREFIX/sbin

# Modify the hadoop-env.sh
RUN sed -i '/^export JAVA_HOME/ s:.*:export JAVA_HOME=/etc/alternatives/jre\nexport HADOOP_PREFIX=/usr/local/hadoop\n:' $HADOOP_CONF_DIR/hadoop-env.sh \
  && sed -i '/^export HADOOP_CONF_DIR/ s:.*:export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop:' $HADOOP_CONF_DIR/hadoop-env.sh

# Copy config
COPY config/* $HADOOP_PREFIX/etc/hadoop/

RUN chmod +x /usr/local/hadoop/etc/hadoop/*-env.sh

# HDFS ports
EXPOSE 50010 50020 50070 50075 50090 8020 9000
# Mapred ports
EXPOSE 10020 19888
# Yarn ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088
# Other ports
EXPOSE 49707 22