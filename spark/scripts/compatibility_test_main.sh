#!/bin/bash

checkSuccess() {
    local test=$1
    # Check for errors in both the short and long-running log files
    if grep -q "AssertionError" "logs/${test}.log"; then
        echo "Test Failed - ${test}"
        return 1  # Indicate failure
    elif (grep -q -e "Exception" "logs/${test}.log" && ! grep -q "SASupportException" "logs/${test}.log"); then
        echo "Test Failed - ${test}"
        return 1  # Indicate failure
    else
        echo "Test Success - ${test}"
        return 0  # Indicate success
    fi
}


SPARK_VERSION=3.4
HUDI_VERSION=1.1.0-SNAPSHOT
# Change jar path, the jars should be placed at ${JARS_PATH}/${spark_version}
JARS_PATH=/Users/ljain/codebase/jars
mkdir -p logs
result_file="logs/compatibility_test_result.txt"
spark_version=${SPARK_VERSION}
test_version=${HUDI_VERSION}
test_jar=${JARS_PATH}/hudi-spark${spark_version}-bundle_2.12-${test_version}.jar,${JARS_PATH}/hudi-cli-bundle_2.12-${test_version}.jar
formatted_test_version=$(echo "$test_version" | sed 's/\./_/g')

# List of from versions we want to run compatibility tests on
# versions_to_check=("0.14.1" "0.14.0" "0.15.0")
versions_to_check=("0.14.1")

function runCompatibilityTest() {
    local from_version=$1
    local test=$2
    if [[ ${test} == *"noupgrade"* ]]; then
        local expected_to_version="6"
    else
        local expected_to_version="8"
    fi

    local formatted_from_version=$(echo "$from_version" | sed 's/\./_/g')
    local formatted_to_version=$(echo "$to_version" | sed 's/\./_/g')

    local test_name="${test}_${formatted_from_version}_${formatted_test_version}"
    echo "Testing ${test} - ${from_version} <> ${test_version}" >> "${result_file}"
    sh compatibility_test.sh -j "${test_jar}" -tv "${test_version}" -fv "${from_version}" -c configs_lock/${test}.props -etv ${expected_to_version} | tee "logs/${test_name}.log" 2>&1
    # Enable this if we want to run long running tests
    # sh compatibility_test_longrunning.sh -j "${test_jar}" -tv "${test_version}" -fv "${from_version}" -c configs/${test}.props > "logs/${test_name}_longrunning.log"
    checkSuccess "${test_name}" >> "${result_file}"
}

# Create properties file for each test case. The name of properties file should end with .props
for from_version in "${versions_to_check[@]}"; do
#  	runCompatibilityTest "${from_version}" "mor_disable_metadata_nonpartitioned_bloom_noupgrade"
#  	runCompatibilityTest "${from_version}" "mor_disable_metadata_nonpartitioned_bloom"
#  	runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned_bloom"
#  	runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned_bloom_noupgrade"
#
#  	runCompatibilityTest "${from_version}" "mor_enable_metadata_nonpartitioned_bloom"
#  	runCompatibilityTest "${from_version}" "mor_enable_metadata_nonpartitioned_bloom_noupgrade"
  	runCompatibilityTest "${from_version}" "mor_enable_metadata_partitioned_bloom"
  	runCompatibilityTest "${from_version}" "mor_enable_metadata_partitioned_bloom_noupgrade"
#
#  	runCompatibilityTest "${from_version}" "mor_disable_metadata_nonpartitioned_noupgrade"
  	runCompatibilityTest "${from_version}" "mor_enable_metadata_nonpartitioned"
#
  	runCompatibilityTest "${from_version}" "mor_enable_metadata_partitioned_rli"
  	runCompatibilityTest "${from_version}" "mor_enable_metadata_partitioned_rli_noupgrade"
  	runCompatibilityTest "${from_version}" "mor_enable_metadata_partitioned_overwrite_payload_rli"

#
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_nonpartitioned"
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned"
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_nonpartitioned_noupgrade"
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned_noupgrade"
    runCompatibilityTest "${from_version}" "mor_enable_metadata_partitioned"
#    runCompatibilityTest "${from_version}" "mor_enable_metadata_nonpartitioned"
    runCompatibilityTest "${from_version}" "mor_enable_metadata_partitioned_noupgrade"
    runCompatibilityTest "${from_version}" "mor_enable_metadata_nonpartitioned_noupgrade"

  	runCompatibilityTest "${from_version}" "cow_enable_metadata_partitioned_bloom"
  	runCompatibilityTest "${from_version}" "cow_enable_metadata_partitioned_bloom_noupgrade"
  	runCompatibilityTest "${from_version}" "cow_enable_metadata_nonpartitioned"
  	runCompatibilityTest "${from_version}" "cow_enable_metadata_partitioned_rli"
  	runCompatibilityTest "${from_version}" "cow_enable_metadata_partitioned_rli_noupgrade"
  	runCompatibilityTest "${from_version}" "cow_enable_metadata_partitioned_overwrite_payload_rli"
    runCompatibilityTest "${from_version}" "cow_enable_metadata_partitioned"
    runCompatibilityTest "${from_version}" "cow_enable_metadata_partitioned_noupgrade"
    runCompatibilityTest "${from_version}" "cow_enable_metadata_nonpartitioned_noupgrade"

#    runCompatibilityTest "${from_version}" "cow_enable_metadata_nonpartitioned"
#    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned"
#    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned_defaultpayload"
#    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned_clustering"
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned_clustering"
#    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned_defaultpayload_clustering"
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned_defaultpayload_clustering"
#    runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned_clustering_noupgrade"
#    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned_clustering_noupgrade"
#    runCompatibilityTest "${from_version}" "cow_enable_metadata_partitioned_noupgrade"
#   runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned_noupgrade2"
done
