 # praas/spark-on-hadoop-hive
DockerHub based main image providing CentOS with Hadoop, Hive and Spark Installed

# Components
 - Centos:latest
 - JDk 8
 - Hadoop 2.7.7
 - Hive 1.2.2
 - Spark 2.4.1

 - Exposed Ports
     + Misc: 6066 7077 8080 8081 4040 4041
     + Hdfs: 50010 50020 50070 50075 50090 8020 9000
     + Mapred: 10020 19888
     + Yarn: 8030 8031 8032 8033 8040 8042 8088
     + Spark: 6066 7077 8080 8081 4040 4041 18080
     + Other: 49707 22

# Versions (tags)
 - sp2.4.1 (Spark 2.4.1)
 - sp3 (Spark 2.3.0)
 - sp2 (Spark 2.2.0)

# Development
Dockerhub repository comes under 'praas' organisation.
Contact admin for getting yourself added as 'Contributer'
Project URL: https://hub.docker.com/r/praas/spark-on-hadoop-hive/

# Instructions for Development
 - **WARN**:  Below steps are needed only when updating the base docker image and should be performed only once by any member.  Once the change is done, everyone can run test normally without these steps.
 - Ensure you have a dockerhub account at:  https://hub.docker.com
 - Ask for adding you as Contributor of 'praas' organisation
 - On your local linux machine, switch to root (`sudo -s`)
 - Log into docker service as:  
   `docker login`
 - To Update/Create a new images, delete all old images first by:  
   `docker rmi <image_id>`  
   **Note:** To get the List of images use: `docker images`
 - Change current directory to `Repo-Base/docker/dockerhub`
 - Before starting the build, we need to update DNS, otherwise the build fails on corporate networks.
   - Run following command to get dns list:
     `nm-tool | grep DNS`  
   - Update following parameter `sudo vim /etc/default/docker`
   - Add the line like below at end of parameter (with whatever DNS list you obtained above):
     `DOCKER_OPTS="--dns 1.1.1.1 --dns 172.30.16.186 --dns 172.30.16.187"`  
   - Restart docker `sudo service docker restart`
   - The above instructions are from:  https://stackoverflow.com/a/28447669/1311255
 - Build the image in the current directory by:  
   `docker build -t praas/spark-on-hadoop-hive:<tag> .`  
   **NOTE:** `praas/spark-on-hadoop-hive` is image name and `sp3` is tag, say, for (spark 2.3.0). This will build the Dockerfile in the current folder, but it will then tag the resulting image.
 - Push to DockerHub by:  
   `docker push praas/spark-on-hadoop-hive:<tag>`
 - Change directory to one-level up and edit the Dockerfile version (with tag) in first line to latest one.  
   e.g.  `FROM praas/spark-on-hadoop-hive:sp3`
 - Run tests normally and Enjoy !!!
