#!/bin/bash

checkSuccess() {
    local test=$1
    local exit_code=$2
    
    if [ $exit_code -eq 0 ]; then
        echo "Test Success - ${test}"
        return 0  # Indicate success
    else
        echo "Test Failed - ${test} (exit code: ${exit_code})"
        return 1  # Indicate failure
    fi
}


export SPARK_HOME=/Users/rahil/spark-3.5
SPARK_VERSION=3.5
HUDI_VERSION=1.1.0-SNAPSHOT
# Change jar path, the jars should be placed at ${JARS_PATH}/${spark_version}
JARS_PATH=/Users/rahil/workplace/hudi/
mkdir -p logs
result_file="logs/compatibility_test_result.txt"
spark_version=${SPARK_VERSION}
test_version=${HUDI_VERSION}
test_jar=${JARS_PATH}/packaging/hudi-spark-bundle/target/hudi-spark${spark_version}-bundle_2.12-${test_version}.jar,${JARS_PATH}/packaging/hudi-cli-bundle/target/hudi-cli-bundle_2.12-${test_version}.jar
formatted_test_version=$(echo "$test_version" | sed 's/\./_/g')

# List of from versions we want to run compatibility tests on
# versions_to_check=("0.14.1" "0.14.0" "0.15.0")
versions_to_check=("1.0.2")

function runCompatibilityTest() {
    local from_version=$1
    local test=$2
    local run_num=${3:-1}
    local expected_to_version="9"

#    if [[ ${test} == *"noupgrade"* ]]; then
#        local expected_to_version="6"
#    else
#        local expected_to_version="8"
#    fi

    local formatted_from_version=$(echo "$from_version" | sed 's/\./_/g')
    local formatted_to_version=$(echo "$to_version" | sed 's/\./_/g')

    local test_name="${test}_${formatted_from_version}_${formatted_test_version}_run${run_num}"
    echo "Testing ${test} - ${from_version} <> ${test_version} (Run ${run_num})" >> "${result_file}"
    sh compatibility_test.sh -j "${test_jar}" -tv "${test_version}" -fv "${from_version}" -c rahil_configs/${test}.props -etv ${expected_to_version} > "logs/${test_name}.log" 2>&1
    local exit_code=$?
    # Enable this if we want to run long running tests
    # sh compatibility_test_longrunning.sh -j "${test_jar}" -tv "${test_version}" -fv "${from_version}" -c configs/${test}.props > "logs/${test_name}_longrunning.log"
    checkSuccess "${test_name}" "${exit_code}" >> "${result_file}"
}

# Create properties file for each test case. The name of properties file should end with .props
for from_version in "${versions_to_check[@]}"; do
    echo "Starting 1 runs for version ${from_version}..." >> "${result_file}"
    for run in {1..1}; do
        echo "=== Run ${run} of 1 ===" >> "${result_file}"
        #runCompatibilityTest "${from_version}" "basic_cow" "${run}"
        #runCompatibilityTest "${from_version}" "basic_mor" "${run}"
#       runCompatibilityTest "${from_version}" "cow_partitioned" "${run}"
#       runCompatibilityTest "${from_version}" "cow_partitioned_defaultPayload" "${run}"
        runCompatibilityTest "${from_version}" "cow_partitioned_metadata_disabled" "${run}"
#       runCompatibilityTest "${from_version}" "cow_partitioned_metadata_enabled" "${run}"
        runCompatibilityTest "${from_version}" "cow_partitioned_metadata_enabled_recordIndexEnabled" "${run}"
#       runCompatibilityTest "${from_version}" "cow_partitioned_metadata_enabled_simpleIndexEnabled" "${run}"
        runCompatibilityTest "${from_version}" "cow_partitioned_metadata_enabled_bloomIndexEnabled" "${run}"
        runCompatibilityTest "${from_version}" "cow_partitioned_OverwriteNonDefaultsWithLatestAvroPayload" "${run}"
#       runCompatibilityTest "${from_version}" "cow_partitioned_overwritePayload" "${run}"
#       runCompatibilityTest "${from_version}" "mor_partitioned" "${run}"
#       runCompatibilityTest "${from_version}" "mor_partitioned_defaultPayload" "${run}"
        runCompatibilityTest "${from_version}" "mor_partitioned_metadata_disabled" "${run}"
#       runCompatibilityTest "${from_version}" "mor_partitioned_metadata_enabled" "${run}"
        runCompatibilityTest "${from_version}" "mor_partitioned_metadata_enabled_recordIndexEnabled" "${run}"
#       runCompatibilityTest "${from_version}" "mor_partitioned_metadata_enabled_simpleIndexEnabled" "${run}"
        runCompatibilityTest "${from_version}" "mor_partitioned_metadata_enabled_bloomIndexEnabled" "${run}"
        runCompatibilityTest "${from_version}" "mor_partitioned_OverwriteNonDefaultsWithLatestAvroPayload" "${run}"
#       runCompatibilityTest "${from_version}" "mor_partitioned_overwritePayload" "${run}"
    done
    echo "Completed 1 runs for version ${from_version}" >> "${result_file}"
done
