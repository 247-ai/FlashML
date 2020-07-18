#!/usr/bin/env bash

# Notes: This script needs to use TAB character instead of 4 spaces, in order to make here documents work.
# Print some information
echo "* This script installs/updates all the dependencies for FlashML for a Debian system."
echo "* Following software are to be installed: OpenJDK 8, mysql, Scala, Hadoop, Spark, Hive, IntelliJ IDEA, Maven."
echo "* Other than OpenJDK and mysql, rest of the software is installed in /opt."
echo ""
echo "* Usage: $0 [options]"
echo "* where, options are one or more from the following: jdk, mysql, scala, hadoop, spark, hive, intellij, maven"
echo "* Default: installs/updates all of the above software from the list."
echo ""
echo ""

# Get the list of software to install


debianVersion=""
debianVersionStr=""
mysqlVariant=""
# Test for debian version
if [[ -e "/etc/debian_version" ]]
then
	versionStr=$(cat /etc/debian_version)
	if [[ ( $versionStr == *"jessie"* ) || ( $versionStr == *"8."* ) ]]
	then
		debianVersion=8
		debianVersionStr="jessie"
		mysqlVariant="mysql"
	elif [[ ( $versionStr == *"stretch"* ) || ( $versionStr == *"9."* ) ]]
	then
		debianVersion=9
		debianVersionStr="stretch"
		mysqlVariant="mariadb"
	elif [[ ( $versionStr == *"buster"* ) || ( $versionStr == *"10."* ) ]]
	then
		debianVersion=10
		debianVersionStr="buster"
		mysqlVariant="mariadb"
	else
		echo "Unsupported Debian version as per /etc/debian_version. Expecting jessie/8, stretch/9 or buster/10."
		exit
	fi
	echo "Debian version: $debianVersion/$debianVersionStr"
else
	echo "Doesn't seem to be a Debian system: could not locate [/etc/debian_version]. This script currently works only for Debian variants."
	exit
fi


# Change to root.
# Using "sudo -i" (interactive login emulation) instead of "sudo -s"
# Using "sudo -i" ensures running root .bashrc, and sets $HOME as /root.
echo "Changing to root"
sudo -i << ROOT

	# Start printing command traces before executing command.
	set -x

	# Install bunch of packages. For installing mysql without interactive mode, we need "debconf-utils".
	apt-get -y update
	apt-get -y upgrade
	apt-get -y install curl openssh-server git ed wget gnupg2 debconf-utils software-properties-common

	# Sleep for 5 secs to make sure the SSH service has started. Not sure if required, but doing anyways.
	echo "Sleeping for 5 secs"
	sleep 5

	# Check for existence of SSHD process, otherwise start.
	isSSHDRunning=`ps -elf | grep sshd | grep -v grep | wc -l`
	if [[ \$isSSHDRunning == 0 ]]; then
		echo "Starting sshd service"
		service ssh restart
		sleep 2  # Sleep for 2 more seconds
	fi

	# Set up SSH for hadoop services
	# First set up an ssh config file
	# Check for the .ssh folder in /root
	[ -e /root/.ssh/ ] || mkdir -p /root/.ssh/

	# Check if config file exists, otherwise skip
	if [[ ! -e /root/.ssh/config ]]
	then
	    echo "Setting up .ssh/config"
		cat <<- EOF > /root/.ssh/config
		Host *
		  UserKnownHostsFile /dev/null
		  StrictHostKeyChecking no
		  LogLevel quiet
		EOF
		chmod 600 /root/.ssh/config
	fi

	# Check if id_rsa related files alreqady exists. If yes, then skip.
	# Note: doesn't seem like ~ refers to /root in AWS EC2 Ubuntu instance. Using full path.
	if [[ ! -e /root/.ssh/id_rsa ]]
	then
		ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
		cat ~/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
		chmod 0600 /root/.ssh/authorized_keys
	fi

	# Switch off command printing
	set +x

	#############################################
	# Now start installation of various software
	#############################################
	# Install MySQL/MariaDB and Java
	if [[ $debianVersion == "8" ]]
	then
		# First check if jdk is installed.
		# For some reason, [[ ]] doesn't work in the below command.
		if ! type javac >/dev/null 2>&1
		then
			# We need to add jessie-backports in the source for openjdk 8
			echo "  Adding jessie-backports for installing openjdk 8"
			deb http://http.debian.net/debian jessie-backports main
			apt-get update
			echo "  Installing openjdk 8"
			apt-get -y install -t jessie-backports openjdk-8-jdk
		else
			echo "# Skipping installation of openjdk 8"
		fi

		# Check if mysql is installed, using mysql binary.
		if ! type mysql >/dev/null 2>&1
		then
			# Install non-interactive mysql with root/root as login/password.
			# First set up  some variables that would be used in the mysql config screen.
			# NOTE: Needs some more testing.
			export DEBIAN_FRONTEND=noninteractive
			sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password password root'
			sudo debconf-set-selections <<< 'mysql-server mysql-server/root_password_again password root'
			# Now install mysql
			echo "  Installing mysql 5.x"
			apt-get -y install mysql-server libmysql-java
		else
			echo "# Skipping installation of mysql 5.x"
		fi
	elif [[ ($debianVersion == "9") || ($debianVersion == "10") ]]
	then
		# First check if jdk is installed.
		# For some reason, [[ ]] doesn't work in the below command.
		if ! type javac >/dev/null 2>&1
		then
			# Install OpenJDK 8 from AdoptOpenJDK repos [https://adoptopenjdk.net/].
			# Set up AdoptOpenJDK repos.
			# Source: https://installvirtual.com/install-java-8-on-debian-10-buster/
			wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add -
			add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/
			apt-get update -y
			echo "  Installing openjdk 8 (adoptopenjdk)"
			apt-get -y install adoptopenjdk-8-hotspot
		else
			echo "# Skipping installation of openjdk 8"
		fi

		# Check if mariadb is installed, using mysql binary.
		if ! type mysql >/dev/null 2>&1
		then
			# Install non-interactive mariadb with root/root as login/password.
			# First set up  some variables that would be used in the mariadb config screen.
			# NOTE: Needs some more testing.
			export DEBIAN_FRONTEND=noninteractive
			sudo debconf-set-selections <<< 'mariadb-server mysql-server/root_password password root'
			sudo debconf-set-selections <<< 'mariadb-server mysql-server/root_password_again password root'
			# Now install mariadb
			echo "  Installing mariadb"
			apt-get -y install mariadb-server libmariadb-java
		else
			echo "# Skipping installation of mariadb"
		fi
	fi


	#############################################################################
	# Install Scala
	SCALA_VERSION="2.12.10"

	if [[ ! -e /opt/scala-\$SCALA_VERSION ]]
	then
		echo " Setting up Scala \$SCALA_VERSION"

		# Note: We don't want variables like SCALA_HOME substituted right away. We want them substituted
		# at the time of execution. So the variables need to be escaped.
		SCALA_HOME=/opt/scala-\$SCALA_VERSION
		SCALA_DOWNLOAD_PATH=scala/\$SCALA_VERSION/scala-\$SCALA_VERSION.tgz
		mkdir -p \${SCALA_HOME}
		echo "  Downloading scala \$SCALA_VERSION"
		curl -#SL https://downloads.lightbend.com/\$SCALA_DOWNLOAD_PATH | tar -xz -C \${SCALA_HOME} --strip-components 1
		echo "  Installing scala \$SCALA_VERSION"
		rm /opt/scala >/dev/null 2>&1
		ln -s \${SCALA_HOME} /opt/scala
	else
		echo "# Skipping installation of Scala \$SCALA_VERSION"
	fi

	#############################################################################
	# Install hadoop
	HADOOP_VERSION=2.9.2

	if [[ ! -e /opt/hadoop-\$HADOOP_VERSION ]]
	then
		echo "Setting up Hadoop \$HADOOP_VERSION"
		echo "  Downloading hadoop \$HADOOP_VERSION"
		curl -#SL https://archive.apache.org/dist/hadoop/common/hadoop-\$HADOOP_VERSION/hadoop-\$HADOOP_VERSION.tar.gz | tar -xz -C /opt/
		echo "  Installing hadoop \$HADOOP_VERSION"
		rm /opt/hadoop >/dev/null 2>&1
		cd /opt/ && ln -s /opt/hadoop-\${HADOOP_VERSION} /opt/hadoop
		cd /opt/hadoop && mkdir -p logs

		# Set up Hadoop config files
		echo "  Setting up hadoop config files"
		# First set up a few env variables to use in this session.
		HADOOP_HOME=/opt/hadoop
		HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop

		# Modify the hadoop-env.sh
		sed -i '/^export JAVA_HOME/ s:.*:export JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64\nexport HADOOP_PREFIX=/opt/hadoop\n:' \$HADOOP_CONF_DIR/hadoop-env.sh
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

		# Detect if we are running on WSL. In that case, add additional configurations.
		if [[ $(grep Microsoft /proc/version) ]]; then
      ed \$HADOOP_CONF_DIR/yarn-site.xml <<- END
      23i
      <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
      </property>
      .
      w
      q
      END
    fi

		# Finally, format the namenode to be ready for use.
		echo "  Formatting Hadoop namenode"
		\$HADOOP_HOME/bin/hdfs namenode -format
	else
		echo "# Skipping installation of Hadoop \$HADOOP_VERSION"
	fi

	#############################################################################
	# Install Hive
	# Updating hive to use 2.3.x version.
	HIVE_VERSION=2.3.6

	if [[ ! -e /opt/hive-\$HIVE_VERSION ]]
	then
		echo "Setting up Hive \$HIVE_VERSION"
		echo "  Downloading Hive \$HIVE_VERSION"
		HIVE_HOME=/opt/hive-\$HIVE_VERSION
		mkdir -p "\${HIVE_HOME}"
		curl -#SL https://archive.apache.org/dist/hive/hive-\$HIVE_VERSION/apache-hive-\$HIVE_VERSION-bin.tar.gz | tar -xz -C \$HIVE_HOME --strip-components 1
		echo "  Installing Hive \$HIVE_VERSION"
		rm /opt/hive >/dev/null 2>&1
		ln -s \$HIVE_HOME /opt/hive

		# Set up mysql as metastore
		ln -s /usr/share/java/mariadb-java-client.jar \$HIVE_HOME/lib/mariadb-java-client.jar
		service mysql start
		mysql -u root -proot -hlocalhost<<- EOF
		CREATE DATABASE metastore;
		USE metastore;
		SOURCE /opt/hive/scripts/metastore/upgrade/mysql/hive-schema-2.3.0.mysql.sql;
		CREATE USER 'hiveuser'@'%' IDENTIFIED BY 'hivepassword';
		GRANT all on *.* to 'hiveuser'@localhost identified by 'hivepassword';
		flush privileges;
		EOF

		# Set the full class name for the db driver
		driverClass=`if [[ $mysqlVariant == "mysql" ]]; then echo "com.mysql.jdbc.Driver"; else echo "org.mariadb.jdbc.Driver"; fi`
		echo "  Using $mysqlVariant JDBC driver: $driverClass"

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
			  <value>$driverClass</value>
			  <description>MySQL/MariaDB JDBC driver class</description>
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
	else
		echo "# Skipping installation of Hive \$HIVE_VERSION"
	fi

	#############################################################################
	# Install Spark
	SPARK_VERSION=2.4.6

	if [[ ! -e /opt/spark-\$SPARK_VERSION ]]
	then
		echo "Setting up Spark \$SPARK_VERSION"
		echo "  Downloading Spark \$SPARK_VERSION"
		SPARK_HOME=/opt/spark-\$SPARK_VERSION
		mkdir -p "\${SPARK_HOME}"
		curl -#SL https://archive.apache.org/dist/spark/spark-\$SPARK_VERSION/spark-\$SPARK_VERSION-bin-hadoop2.7.tgz | tar -xz -C \$SPARK_HOME --strip-components 1
		echo "  Installing Spark \$SPARK_VERSION"
		rm /opt/spark >/dev/null 2>&1
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
	else
		echo "# Skipping installation of Spark \$SPARK_VERSION"
	fi

	#############################################################################
	# Install IntelliJ Idea
	IDEA_VERSION=2020.1

	if [[ ! -e /opt/idea-\$IDEA_VERSION ]]
	then
		echo "Setting up IntelliJ IDEA \$IDEA_VERSION"
		echo "  Downloading IntelliJ IDEA \$IDEA_VERSION"
		IDEA_HOME=/opt/idea-\$IDEA_VERSION
		mkdir -p "\${IDEA_HOME}"
		curl -#SL https://download.jetbrains.com/idea/ideaIC-\$IDEA_VERSION-no-jbr.tar.gz | tar -xz -C \$IDEA_HOME --strip-components 1
		echo "  Installing IntelliJ IDEA \$IDEA_VERSION"
		rm /opt/idea >/dev/null 2>&1
		ln -s \$IDEA_HOME /opt/idea
	else
		echo "# Skipping installation of IntelliJ IDEA \$IDEA_VERSION"
	fi

	##############################################################################
	# Install maven
	MAVEN_VERSION=3.6.2

	if [[ ! -e /opt/maven-\$MAVEN_VERSION ]]
	then
		echo "Setting up Maven \$MAVEN_VERSION"
		echo "  Downloading Maven \$MAVEN_VERSION"
		MAVEN_HOME=/opt/maven-\$MAVEN_VERSION
		mkdir -p "\${MAVEN_HOME}"
		curl -#SL https://archive.apache.org/dist/maven/maven-3/\$MAVEN_VERSION/binaries/apache-maven-\$MAVEN_VERSION-bin.tar.gz | tar -xz -C \$MAVEN_HOME --strip-components 1
		echo "  Installing Maven \$MAVEN_VERSION"
		rm /opt/maven >/dev/null 2>&1
		ln -s \$MAVEN_HOME /opt/maven
	else
		echo "# Skipping installation of Maven \$MAVEN_VERSION"
	fi

	##############################################################################
	# Finally set the PATH variable for root accont.

	echo "  Modifying root .bashrc"
	cat <<- EOF >> /root/.bashrc

	# Set up JAVA_HOME
	export JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64
	# Set up HADOOP_HOME
	export HADOOP_HOME=/opt/hadoop
	export HADOOP_CONF_DIR=\\\$HADOOP_HOME/etc/hadoop
	# Set up Spark variables (needed for Zepplin)
	#export SPARK_HOME=/opt/spark

	# Update PATH
	export PATH=\\\$JAVA_HOME/bin:\\\$HADOOP_HOME/bin:\\\$HADOOP_HOME/sbin:/opt/scala/bin:/opt/spark/bin:/opt/hive/bin:\\\$PATH
	EOF

	echo "Installation complete"
ROOT

# Now modify the user .bashrc as well
echo "  Modifying user .bashrc"
cat << EOF >> ~/.bashrc

# Set up JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64
# Set up HADOOP_HOME
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
# Setup Maven
export M2_HOME=/opt/maven

# Update PATH
export PATH=\$JAVA_HOME/bin:\$HADOOP_HOME/bin:/opt/scala/bin:/opt/spark/bin:/opt/hive/bin:\${M2_HOME}/bin:/opt/idea/bin:\$PATH
EOF

echo "Please reboot the system or restart WSL to activate software paths!!"
