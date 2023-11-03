#!/bin/bash

# Set the Flink CLI location
FLINK_CLI="${FLINK_HOME}/bin/flink"

# List all running jobs and get their Job IDs
running_jobs=$($FLINK_CLI list | grep RUNNING | awk '{print $4}')

# Loop through the Job IDs and cancel each job
for job_id in $running_jobs; do
    $FLINK_CLI cancel $job_id
done