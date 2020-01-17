#!/bin/bash -e

# First build the docker image from the dockerfile
sudo docker build -t flashml ./

# Hostname for docker container
dockerHostname="flashml-docker"

# Set up the DockerFile so that we can run the tests if the "test" argument is provided,
# otherwise drop into a shell in the container.
if [[ "$1" == "test" ]]
then
    # Find the names of main and test jar
    # First find out if we have multiple versions of the main JAR available. In that case, we will exit with an error.
    if [[ `ls FlashML-*-tests.jar | wc -l` > 1 ]]
    then
		    echo "Multiple versions of FlashML jar detected in this folder. Please remove old versions of main and test jars."
		    exit -1
    fi
    mainJar=`ls FlashML*[^tests].jar`
    testJar=`ls FlashML-*-tests.jar`
    echo "** Using [$mainJar] and [$testJar] for running tests"

    # Now create a docker container by running the image
    # -h: Sets the hostname
    # --rm: Removes the container after exit
    # -it: Keep STDIN open even if not attached and allocate a pseudo-tty
    # --name: Gives a name to the container
    # Note: name of the image has to be after --entrypoint parameter.
    # Note: Also echoing the command
    set -x
    sudo docker run -h $dockerHostname \
			  -p 18080:18080 \
    		--rm \
    		-it \
    		--name flashmlcontainer \
    		--entrypoint /FlashML/run-flashml-tests.sh \
    		flashml \
    		$mainJar $testJar $@
else
    echo "** Dropping into bash with all dependencies"
    # Using getopts to get command line arguments
    hostFolder=""
    mntFolder="/FlashML/project"
    mountCmd=""
    while getopts ":m:" Opt
    do
      case $Opt in
        m )
          echo "** Mounting folder ${OPTARG} to /project"
          hostFolder=$OPTARG
          mountCmd="--mount type=bind,source=$hostFolder,target=$mntFolder";;
        * ) echo "Unknown option $Opt";;
      esac
    done
    # Now create a docker container by running the image
    # For options, see above.
    # Note: name of the image has to be after --entrypoint parameter.
    # Note: Also echoing the command using "-x"
    set -x
    sudo docker run -h $dockerHostname \
	    	-p 18080:18080 \
    		--rm \
    		-it \
    		--name flashmlcontainer \
			$mountCmd \
			--entrypoint /FlashML/run-flashml.sh \
			flashml
fi

# Not removing docker image
#sudo docker rmi flashml