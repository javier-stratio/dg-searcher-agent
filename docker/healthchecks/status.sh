#!/usr/bin/env bash

res=$(grep 'org.apache.http.NoHttpResponseException' /mnt/mesos/sandbox/stderr | tail -60 | wc -l)

if [ "$res" != "0" ]; then
       echo "ERROR: The Searcher agent has found some connection problems and will be restarted."
       exit 1
fi
