#!/bin/bash

IDS=$1  # e.g. "1,2,3,4,5"

sum=0
IFS=',' read -ra id_list <<< "$IDS"
for id in "${id_list[@]}"; do
    sum=$((sum + id))
done

echo "Bash sum: $sum"
