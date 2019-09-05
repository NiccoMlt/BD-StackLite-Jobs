#!/usr/bin/env bash

OUTPUTFILE =~/performance

for executors in $(seq 1 9); do
  for part in $(seq 1 9); do
    echo "JOB DONE WITH EXECUTORS: $executors AND PARTITIONS: $part";
    spark2-submit ./director-actor-job-2.1.2-spark.jar JOB1 $executors $part > OUTPUTFILE$executors$part 2>&1;
  done;
done

for executors in $(seq 1 9); do
  for part in $(seq 1 9); do
    echo "JOB DONE WITH EXECUTORS: $executors AND PARTITIONS: $part";
    grep -i --regexp="Job [0-9][0-9]* finished:" OUTPUTFILE$executors$part;
  done;
done
