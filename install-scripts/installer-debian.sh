#!/usr/bin/env bash

# Notes: This script needs to use TAB character instead of 4 spaces, in order to make here documents work.
# Print some information
echo "This script installs/updates all the dependencies for FlashML for a Debian system."
echo "Following software are to be installed: OpenJDK 8, mysql, Scala, Hadoop, Spark, Hive, IntelliJ IDEA, Maven."
echo "Other than OpenJDK and mysql, rest of the software is installed in /opt."

debianVersion=""
# Test for debian version
if [[ -e "/etc/debian_version" ]]
then
	versionStr=$(cat /etc/debian_version)
	if [[ ( $versionStr == *"jessie"* ) || ( $versionStr == *"8."* ) ]]
		then debianVersion=8
	elif [[ ( $versionStr == *"stretch"* ) || ( $versionStr == *"9."* ) ]]
		then debianVersion=9
	elif [[ ( $versionStr == *"buster"* ) || ( $versionStr == *"10."* ) ]]
		then debianVersion=10
	else
		echo "Unsupported Debian version as per /etc/debian_version. Expecting jessie/8 or stretch/9."
		exit
	fi
	echo "Debian version: $debianVersion"
else
	echo "Doesn't seem to be a Debian system: could locate /etc/debian_version"
	exit
fi

# Change to root
echo "Changing to root"
sudo -s << ROOT

	# Start printing command traces before executing command.
	set -x
	# Install bunch of packages
	apt-get -y update
	apt-get -y upgrade
	apt-get -y install curl openssh-server git ed

	# Set up SSH for hadoop services
	# First set up an ssh config file
	# Check for the .ssh folder in /root
	[ -e /root/.ssh/ ] || mkdir -p /root/.ssh/
	cat <<- EOF > /root/.ssh/config
	Host *
	  UserKnownHostsFile /dev/null
	  StrictHostKeyChecking no
	  LogLevel quiet
	EOF
	chmod 600 /root/.ssh/config

	ssh-keygen -q -N "" -t rsa -f ~/.ssh/id_rsa
	cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
	chmod 0600 ~/.ssh/authorized_keys

	# Switch off command printing
	set +x

	# Now start installation of various software
	###########################################
	# MySQL and Java
	echo "Installing OpenJDK 8 and mysql 5"

	if [[ $debianVersion == "8" ]]
	then
		# We need to add jessie-backports in the source for openjdk 8
		echo "  Adding jessie-backports for installing openjdk 8"
		deb http://http.debian.net/debian jessie-backports main
		apt-get update
		apt-get -y install -t jessie-backports openjdk-8-jdk
	elif [[ ($debianVersion == "9") || ($debianVersion == "10") ]]
	then
		# Install JDK 8
		apt-get -y install openjdk-8-jdk
		# Set up mysql repos
		echo -e "deb http://repo.mysql.com/apt/debian/ stretch mysql-5.7\ndeb-src http://repo.mysql.com/apt/debian/ stretch mysql-5.7" > /etc/apt/sources.list.d/mysql.list
		wget -O /tmp/RPM-GPG-KEY-mysql https://repo.mysql.com/RPM-GPG-KEY-mysql
		apt-key add /tmp/RPM-GPG-KEY-mysql
		apt-get update
	fi

	# Install non-interactive mysql with root/root as login/password.
	# First set up  some variables that would be used in the mysql config screen.
	# NOTE: Needs some more testing.
	sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password password root'
	sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password_again password root'
	# Now install mysql
	apt-get -y install mysql-server libmysql-java

	#############################################################################
	# Install Scala
	SCALA_VERSION="2.11.12"
	echo "Setting up Scala \$SCALA_VERSION"

	# Note: We don't want variables like SCALA_HOME substituted right away. I want them substituted
	# at the time of execution. So the variables need to be escaped.
	SCALA_HOME=/opt/scala-\$SCALA_VERSION
	SCALA_DOWNLOAD_PATH=scala/\$SCALA_VERSION/scala-\$SCALA_VERSION.tgz
	mkdir -p \${SCALA_HOME}
	echo "  Downloading scala \$SCALA_VERSION"
	curl -#SL https://downloads.lightbend.com/\$SCALA_DOWNLOAD_PATH | tar -xz -C \${SCALA_HOME} --strip-components 1
	echo "  Installing scala \$SCALA_VERSION"
	ln -s \${SCALA_HOME} /opt/scala

	#############################################################################
	# Install hadoop
	HADOOP_VERSION=2.7.3
	echo "Setting up Hadoop \$HADOOP_VERSION"
	echo "  Downloading hadoop \$HADOOP_VERSION"
	curl -#SL https://archive.apache.org/dist/hadoop/common/hadoop-\$HADOOP_VERSION/hadoop-\$HADOOP_VERSION.tar.gz | tar -xz -C /opt/
	echo "  Installing hadoop \$HADOOP_VERSION"
	cd /opt/ && ln -s /opt/hadoop-\${HADOOP_VERSION} /opt/hadoop
	cd /opt/hadoop && mkdir -p logs

	# Set up Hadoop config files
	echo "  Setting up hadoop config files"
	# Modify the hadoop-env.sh
	HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
	sed -i '/^export JAVA_HOME/ s:.*:export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64\nexport HADOOP_PREFIX=/opt/hadoop\n:' \$HADOOP_CONF_DIR/hadoop-env.sh
	sed -i '/^export HADOOP_CONF_DIR/ s:.*:export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop:' \$HADOOP_CONF_DIR/hadoop-env.sh
	ed \$HADOOP_CONF_DIR/hadoop-env.sh <<- END
	35i
	export HADOOP_INSTALL=\\\$HADOOP_HOME
	export HADOOP_MAPRED_HOME=\\\$HADOOP_HOME
	export HADOOP_COMMON_HOME=\\\$HADOOP_HOME
	export HADOOP_HDFS_HOME=\\\$HADOOP_HOME
	export YARN_HOME=\\\$HADOOP_HOME
	export HADOOP_COMMON_LIB_NATIVE_DIR=\\\$HADOOP_HOME/lib/native
	export HADOOP_OPTS="\\\${HADOOP_OPTS} -Djava.library.path=\\\$HADOOP_PREFIX/lib"
	.
	w
	q
	END
	chmod +x \$HADOOP_CONF_DIR/*-env.sh

	# Modify other configuration files
	ed \$HADOOP_CONF_DIR/core-site.xml <<- END
	20i
	<property>
	  <name>fs.default.name</name>
		<value>hdfs://localhost:9000</value>
	</property>
	.
	w
	q
	END

	ed \$HADOOP_CONF_DIR/hdfs-site.xml <<- END
	20i
	<property>
	 <name>dfs.replication</name>
	 <value>1</value>
	</property>

	<property>
	  <name>dfs.name.dir</name>
	  <value>file:///opt/hadoop/hadoopdata/hdfs/namenode</value>
	</property>

	<property>
	  <name>dfs.data.dir</name>
	  <value>file:///opt/hadoop/hadoopdata/hdfs/datanode</value>
	</property>

	<property>
	  <name>dfs.permissions</name>
	  <value>false</value>
	</property>
	.
	w
	q
	END

	cp \$HADOOP_CONF_DIR/mapred-site.xml.template \$HADOOP_CONF_DIR/mapred-site.xml
	ed \$HADOOP_CONF_DIR/mapred-site.xml <<- END
	20i
	<property>
	  <name>mapreduce.framework.name</name>
		<value>yarn</value>
	</property>
	.
	w
	q
	END

	ed \$HADOOP_CONF_DIR/yarn-site.xml <<- END
	18i
	<property>
	  <name>yarn.nodemanager.aux-services</name>
		<value>mapreduce_shuffle</value>
	</property>
	.
	w
	q
	END

	# Finally format the namenode
  echo "  Formatting Hadoop namenode"
	/opt/hadoop/bin/hdfs namenode -format

	#############################################################################
	# Install Hive
	HIVE_VERSION=1.2.2

	echo "Setting up Hive \$HIVE_VERSION"
	echo "  Downloading Hive \$HIVE_VERSION"
	HIVE_HOME=/opt/hive-\$HIVE_VERSION
	mkdir -p "\${HIVE_HOME}"
	curl -#SL https://archive.apache.org/dist/hive/hive-\$HIVE_VERSION/apache-hive-\$HIVE_VERSION-bin.tar.gz | tar -xz -C \$HIVE_HOME --strip-components 1
	echo "  Installing Hive \$HIVE_VERSION"
	ln -s \$HIVE_HOME /opt/hive

	# Set up mysql as metastore
	ln -s /usr/share/java/mysql-connector-java.jar \$HIVE_HOME/lib/mysql-connector-java.jar
	service mysql start
	mysql -u root -proot -hlocalhost<<- EOF
	CREATE DATABASE metastore;
	USE metastore;
	SOURCE /opt/hive/scripts/metastore/upgrade/mysql/hive-schema-0.14.0.mysql.sql;
	CREATE USER 'hiveuser'@'%' IDENTIFIED BY 'hivepassword';
	GRANT all on *.* to 'hiveuser'@localhost identified by 'hivepassword';
	flush privileges;
	EOF

	# Set up Hive configs
	cat <<- EOF > \$HIVE_HOME/conf/hive-site.xml
	<configuration>
	   <property>
		  <name>javax.jdo.option.ConnectionURL</name>
		  <value>jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true&amp;useSSL=false</value>
		  <description>metadata is stored in a MySQL server</description>
	   </property>
	   <property>
		  <name>javax.jdo.option.ConnectionDriverName</name>
		  <value>com.mysql.jdbc.Driver</value>
		  <description>MySQL JDBC driver class</description>
	   </property>
	   <property>
		  <name>javax.jdo.option.ConnectionUserName</name>
		  <value>hiveuser</value>
		  <description>user name for connecting to mysql server</description>
	   </property>
	   <property>
		  <name>javax.jdo.option.ConnectionPassword</name>
		  <value>hivepassword</value>
		  <description>password for connecting to mysql server</description>
	   </property>
	</configuration>
	EOF

	#############################################################################
	# Install Spark
	SPARK_VERSION=2.4.4

	echo "Setting up Spark \$SPARK_VERSION"
	echo "  Downloading Spark \$SPARK_VERSION"
	SPARK_HOME=/opt/spark-\$SPARK_VERSION
	mkdir -p "\${SPARK_HOME}"
	curl -#SL https://archive.apache.org/dist/spark/spark-\$SPARK_VERSION/spark-\$SPARK_VERSION-bin-hadoop2.7.tgz | tar -xz -C \$SPARK_HOME --strip-components 1
	echo "  Installing Spark \$SPARK_VERSION"
	ln -s \$SPARK_HOME /opt/spark

  # Create a symlink for hive-site.xml file in the conf folder
  ln -s /opt/hive/conf/hive-site.xml hive-site.xml

	# Set up the spark-env.sh file
	cat <<- EOF > /opt/spark/conf/spark-env.sh
	#!/usr/bin/env bash
	export HADOOP_CONF_DIR=\\\$HADOOP_HOME/etc/hadoop
	export YARN_CONF_DIR=\\\$HADOOP_CONF_DIR
	EOF
	chmod +x /opt/spark/conf/spark-env.sh

	#############################################################################
	# Install IntelliJ Idea
	IDEA_VERSION=2019.2

	echo "Setting up IntelliJ IDEA \$IDEA_VERSION"
	echo "  Downloading IntelliJ IDEA \$IDEA_VERSION"
	IDEA_HOME=/opt/idea-\$IDEA_VERSION
	mkdir -p "\${IDEA_HOME}"
	curl -#SL https://download.jetbrains.com/idea/ideaIC-\$IDEA_VERSION-no-jbr.tar.gz | tar -xz -C \$IDEA_HOME --strip-components 1
	echo "  Installing IntelliJ IDEA \$IDEA_VERSION"
	ln -s \$IDEA_HOME /opt/idea

	##############################################################################
	# Install maven
	MAVEN_VERSION=3.6.2

	echo "Setting up Maven \$MAVEN_VERSION"
	echo "  Downloading Maven \$MAVEN_VERSION"
	MAVEN_HOME=/opt/maven-\$MAVEN_VERSION
	mkdir -p "\${MAVEN_HOME}"
	curl -#SL https://archive.apache.org/dist/maven/maven-3/\$MAVEN_VERSION/binaries/apache-maven-\$MAVEN_VERSION-bin.tar.gz | tar -xz -C \$MAVEN_HOME --strip-components 1
	echo "  Installing Maven \$MAVEN_VERSION"
	ln -s \$MAVEN_HOME /opt/maven

	##############################################################################
	# Finally set the PATH variable

	echo "  Modifying root .bashrc"
	cat <<- EOF >> /root/.bashrc

  # Set up JAVA_HOME
  export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
  # Set up HADOOP_HOME
  export HADOOP_HOME=/opt/hadoop
  export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop

	# Update PATH
	export PATH=\\\$PATH:\\\$HADOOP_HOME/bin:\\\$HADOOP_HOME/sbin:/opt/scala/bin:/opt/spark/bin:/opt/hive/bin
	EOF

	echo "Installation complete"
ROOT

# Now modify the user .bashrc as well
echo "  Modifying user .bashrc"
cat << EOF >> ~/.bashrc

# Set up JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
# Set up HADOOP_HOME
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
# Setup Maven
export M2_HOME=/opt/maven

# Update PATH
export PATH=\$PATH:\$HADOOP_HOME/bin:/opt/scala/bin:/opt/spark/bin:/opt/hive/bin:\${M2_HOME}/bin:/opt/idea/bin
EOF

echo "Please reboot the system to activate software paths!!"
