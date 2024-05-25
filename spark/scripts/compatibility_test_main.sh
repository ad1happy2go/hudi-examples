#!/bin/bash

function checkSuccess() {
    local test=$1
    if grep -q "java.lang.AssertionError" "logs/${test}.log"; then
        echo "Test Failed - ${test}"
    else
        echo "Test Success - ${test}"
    fi
}
mkdir -p logs
result_file="logs/compatibility_test_result.txt"
export SPARK_HOME=/Users/adityagoenka/spark/spark-3.2.3-bin-hadoop3.2
export JAR_PATH=/Users/adityagoenka/docker_demo/artifacts/jars/0.15.0-rc1/3.2/hudi-spark3.2-bundle_2.12-0.15.0-rc1.jar


test_version="0.15.0-rc1"
formatted_test_version=$(echo "$test_version" | sed 's/\./_/g')
versions_to_check=("0.14.1" "0.14.0" "0.13.1" "0.13.0" "0.12.3")

function runCompatibilityTest() {
    local from_version=$1
    local test=$2

    local formatted_from_version=$(echo "$from_version" | sed 's/\./_/g')
    local formatted_to_version=$(echo "$to_version" | sed 's/\./_/g')

    local test_name="${test}_${formatted_from_version}_${formatted_test_version}"
    echo "Testing ${test} - ${from_version} <> ${test_version}" >> "${result_file}"
    sh compatibility_test.sh -j "${JAR_PATH}" -tv "${test_version}" -fv "${from_version}" -c configs/${test}.props > "logs/${test_name}.log"
    checkSuccess "${test_name}" >> "${result_file}"
}


for from_version in "${versions_to_check[@]}"; do
    # runCompatibilityTest "${from_version}" "cow_enable_metadata_nonpartitioned"
    # runCompatibilityTest "${from_version}" "mor_disable_metadata_nonpartitioned"
    # runCompatibilityTest "${from_version}" "mor_enable_metadata_partitioned"
    # runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned"
    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned"
done
