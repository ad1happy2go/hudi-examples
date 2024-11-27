#!/bin/bash

checkSuccess() {
    local test=$1
    if grep -q "AssertionError" "logs/${test}.log"; then
        echo "Test Failed - ${test}"
        return 1  # Indicate failure
    elif grep -q -e "Exception" "logs/${test}.log" && ! grep -q "SASupportException" "logs/${test}.log"; then
        echo "Test Failed - ${test}"
        return 1  # Indicate failure
    else
        echo "Test Success - ${test}"
        return 0  # Indicate success
    fi
}


mkdir -p logs
result_file="logs/compatibility_test_result.txt"
spark_version=${SPARK_VERSION}
test_version=${HUDI_VERSION}
test_jar="/Users/sagars/hudi/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.0-SNAPSHOT.jar,/Users/sagars/hudi/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.0-SNAPSHOT.jar,/Users/sagars/hudi/packaging/hudi-cli-bundle/target/hudi-cli-bundle_2.12-1.0.0-SNAPSHOT.jar"
formatted_test_version=$(echo "$test_version" | sed 's/\./_/g')

# versions_to_check=("0.14.1" "0.14.0" "0.13.1" "0.13.0" "0.12.3")
versions_to_check=("0.15.0")

function runCompatibilityTest() {
    local from_version=$1
    local test=$2

    local formatted_from_version=$(echo "$from_version" | sed 's/\./_/g')
    local formatted_to_version=$(echo "$to_version" | sed 's/\./_/g')

    local test_name="${test}_${formatted_from_version}_${formatted_test_version}"
    echo "Testing ${test} - ${from_version} <> ${test_version}" >> "${result_file}"
    sh compatibility_test.sh -j "${test_jar}" -tv "${test_version}" -fv "${from_version}" -c configs/${test}.props > "logs/${test_name}.log"
    checkSuccess "${test_name}" >> "${result_file}"
}


for from_version in "${versions_to_check[@]}"; do
    # runCompatibilityTest "${from_version}" "cow_enable_metadata_nonpartitioned"
    # runCompatibilityTest "${from_version}" "mor_disable_metadata_nonpartitioned"
    # runCompatibilityTest "${from_version}" "mor_enable_metadata_partitioned"
    # runCompatibilityTest "${from_version}" "mor_disable_metadata_partitioned"
    runCompatibilityTest "${from_version}" "cow_disable_metadata_partitioned"
done
