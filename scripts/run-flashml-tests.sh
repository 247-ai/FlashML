#!/bin/bash

# The script takes the following arguments:
# 1: Path of Main jar, e.g., flashml-2018.4-SNAPSHOT.jar
# 2: Path of Test jar, e.g., flashml-2018.4-SNAPSHOT-tests.jar

# Print the complete execution command for this script
myInvocation="$(printf %q "$BASH_SOURCE")$((($#)) && printf ' %q' "$@")"
echo "** Executing: $myInvocation"
echo ""

if [[ $# -eq 0 ]]
then
    echo "Usage: run_flashml_tests.sh /path/to/main/FlashML/jar /path/to/test/FlashML/jar [test name]"
    echo "test name: Option argument, provide only the test name (e.g., BinaryNBTest), not the FQN."
    echo "           Default: run all tests."
    exit 0
fi

# Set up the classpath
mainJar=$1
testJar=$2
alljars="docker/scalatest_2.12-3.1.0.jar:docker/scalactic_2.12-3.1.0.jar:$mainJar"

# We will keep track of time it takes to run all the tests.
SECONDS=0
# Check if we are running a specific test, otherwise run all tests
if [[ $# -eq 3 ]]
then
    scala -J-Xmx2g -cp "$alljars" org.scalatest.tools.Runner -o -R $testJar -s com.tfs.flashml.$3
else
    echo "** Starting all tests"
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
      scala -J-Xmx2g -cp "$alljars" org.scalatest.tools.Runner -o -R $testJar -s com.tfs.flashml.$testClass
      RESULT=$?
      if [ ! $RESULT -eq 0 ]; then
        failedTest+=$testClass$'\n'
      fi
    done
fi

# Print time taken
# $SECONDS is an inbuilt function in bash to keep track of time.
duration=$SECONDS
echo "** Time taken to run all tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds."

# Report failed tests
if [ ! -z "$failedTest" ]; then
    echo "Failed Tests:"
    echo "$failedTest"
fi
