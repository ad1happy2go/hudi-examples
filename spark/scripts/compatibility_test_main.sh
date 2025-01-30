#!/bin/bash

checkSuccess() {
    local test=$1
    # Check for errors in both the short and long-running log files
    if grep -q "AssertionError" "logs/${test}.log" || grep -q "AssertionError" "logs/${test}_longrunning.log"; then
        echo "Test Failed - ${test}"
        return 1  # Indicate failure
    elif (grep -q -e "Exception" "logs/${test}.log" && ! grep -q "SASupportException" "logs/${test}.log") || \
         (grep -q -e "Exception" "logs/${test}_longrunning.log" && ! grep -q "SASupportException" "logs/${test}_longrunning.log"); then
        echo "Test Failed - ${test}"
        return 1  # Indicate failure
    else
        echo "Test Success - ${test}"
        return 0  # Indicate success
    fi
}


SPARK_VERSION=3.4
HUDI_VERSION=1.0.0-SNAPSHOT
# Change jar path, the jars should be placed at ${JARS_PATH}/${spark_version}
JARS_PATH=/Users/adityagoenka/jars/1.0.0-SNAPSHOT/siva_0512
mkdir -p logs
result_file="logs/compatibility_test_result.txt"
spark_version=${SPARK_VERSION}
test_version=${HUDI_VERSION}
test_jar=${JARS_PATH}/${spark_version}/hudi-spark${spark_version}-bundle_2.12-${test_version}.jar,${JARS_PATH}/${spark_version}/hudi-cli-bundle_2.12-${test_version}.jar
formatted_test_version=$(echo "$test_version" | sed 's/\./_/g')

# List of from versions we want to run compatibility tests on
# versions_to_check=("0.14.1" "0.14.0" "0.15.0")
versions_to_check=("0.15.0")

function runCompatibilityTest() {
    local from_version=$1
    local test=$2

    local formatted_from_version=$(echo "$from_version" | sed 's/\./_/g')
    local formatted_to_version=$(echo "$to_version" | sed 's/\./_/g')

    local test_name="${test}_${formatted_from_version}_${formatted_test_version}"
    echo "Testing ${test} - ${from_version} <> ${test_version}" >> "${result_file}"
    sh compatibility_test.sh -j "${test_jar}" -tv "${test_version}" -fv "${from_version}" -c configs/${test}.props > "logs/${test_name}.log"
    # Enable this if we want to run long running tests
    # sh compatibility_test_longrunning.sh -j "${test_jar}" -tv "${test_version}" -fv "${from_version}" -c configs/${test}.props > "logs/${test_name}_longrunning.log"
    checkSuccess "${test_name}" >> "${result_file}"
}

# Create properties file for each test case. The name of properties file should end with .props
for from_version in "${versions_to_check[@]}"; do
#    runCompatibilityTest "${from_version}" "cow_enable_metadata_nonpartitioned"
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_nonpartitioned"
#    runCompatibilityTest "${from_version}" "mor_enable_metadata_partitioned"
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned"
#    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned"
#    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned_defaultpayload"
#    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned_clustering"
    runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned_clustering"
#    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned_defaultpayload_clustering"
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned_defaultpayload_clustering"
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned_clustering_noupgrade"
#    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned_clustering_noupgrade"
#    runCompatibilityTest "${from_version}" "cow_enable_metadata_partitioned_noupgrade"
#    runCompatibilityTest "${from_version}" "mor_enable_metadata_partitioned_noupgrade"
#    runCompatibilityTest "${from_version}" "mor_enable_metadata_non_partitioned_noupgrade"
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned_noupgrade"
done
