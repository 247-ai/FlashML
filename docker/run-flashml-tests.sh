#!/bin/bash

# The script takes the following arguments:
# 1: Main jar name, e.g., flashml-2018.4-SNAPSHOT.jar
# 2: Test jar name, e.g., flashml-2018.4-SNAPSHOT-tests.jar
# 3: Additional argument(s) that were passed to run-docker.sh

# Print the complete execution command for this script
myInvocation="$(printf %q "$BASH_SOURCE")$((($#)) && printf ' %q' "$@")"
echo "** Executing: $myInvocation"
echo ""

/usr/sbin/sshd
hdfs namenode -format
start-dfs.sh
start-yarn.sh
echo "** Started hadoop"

# This folder is used for logging spark-events
mkdir -p /tmp/spark-events

# Load data in hive
hive -f scripts/titanic-survival-data.sql
hive -f scripts/web_journey_data_load.sql
hive -f scripts/yelp_1k.sql
hive -f scripts/yelp_10k.sql
echo "** Loaded data into hive"

hadoop fs -mkdir /data
hadoop fs -put data/* /data/
echo "** Loaded yelp datasets to hdfs at /data"

hive --service metastore &
echo "** Started hive metastore"


if [[ $3 == "test" ]]
then
    # Set up the classpath
    mainJar=$1
    testJar=$2
    alljars="scalatest_2.11-3.0.5.jar:scalactic_2.11-3.0.5.jar:$mainJar"
    extraDockerOptions="-Dhdfs.nameNode.uri=hdfs://flashml-docker:9000 -Dhive.thrift.url=thrift://flashml-docker:9083"

    # We will keep track of time it takes to run all the tests.
    SECONDS=0
    # Check if we are running a specific test, otherwise run all tests
    if [[ $# -eq 4 ]]
    then
        scala -J-Xmx2g -cp "$alljars" org.scalatest.tools.Runner -o -R $testJar -s com.tfs.flashml.$4 $extraDockerOptions
    else
        echo "** Starting all tests"
        # We will keep track of time it takes to run all the tests.
        SECONDS=0
        # List of tests to run.
        # Add new tests in this list.
        testList=(\
            systemTests.BinaryDecisionTreeTest \
            systemTests.BinaryGBTCVTest \
            systemTests.BinaryGBTHyperband \
            systemTests.BinaryGBTTest \
            systemTests.BinaryLogisticTest \
        	  systemTests.BinaryLRBinningNoPageTest \
            systemTests.BinaryNBTest \
            systemTests.BinaryRandomForestTest \
            systemTests.BinarySVMPGBinningAllPageTest \
            systemTests.BinarySVMPgBinningTest \
            systemTests.BinarySVMPgTest \
            systemTests.BinarySVMPgUpliftTest \
            systemTests.BinarySVMTest \
            systemTests.BinarySVMUpliftTest \
            systemTests.MultiIntentDecisionTreeTest \
            systemTests.MultiIntentDTCVTest \
            systemTests.MultiIntentLRHyperBandTest \
            systemTests.MultiIntentLRNullCheck \
            systemTests.MultiIntentLRRandomSamplingTest \
            systemTests.MultiIntentLRStratifiedSamplingTest \
            systemTests.MultiIntentMLPCVTest \
            systemTests.MultiIntentMLPTest \
            systemTests.MultiIntentNBCVTest \
            systemTests.MultiIntentNBTest \
            systemTests.MultiIntentRFCVTest \
            systemTests.MultiIntentRFTest \
            systemTests.MultiIntentSVMCVTest \
            systemTests.MultiIntentSVMHyperBandTest \
            systemTests.MultiIntentSVMStdMetricsTest \
            systemTests.MultiIntentSVMTest \
	          functionalTests.PlattScalerWithTopKTest \
            functionalTests.BinaryLogisticWithDoubleResponseTest \
            functionalTests.DataReaderTest \
            functionalTests.MonitoringMetricsTest \
            functionalTests.PreprocessingTest \
            functionalTests.ConfigValidatorPositiveTest \
            functionalTests.ConfigValidatorNegativeTest \
            functionalTests.ConfigValidatorNegativeTest2 \
        )
        failedTest=""
        for testClass in ${testList[@]}
        do
			    scala -J-Xmx2g -cp "$alljars" org.scalatest.tools.Runner -o -R $testJar -s com.tfs.flashml.$testClass $extraDockerOptions
          RESULT=$?
          if [ ! $RESULT -eq 0 ]; then
            failedTest+=$testClass$'\n'
          fi
		    done
    fi
	# Print time taken
	duration=$SECONDS
	echo "** Time taken to run all tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds."

	#Fail the build if there was an error
  if [ ! -z "$failedTest" ]; then
    echo "Failed Tests:"
    echo "$failedTest"
    exit -1
  fi
else
        echo "** No tests ran"
fi
