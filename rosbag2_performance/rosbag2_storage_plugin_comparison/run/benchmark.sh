#!/usr/bin/env bash

set -e # Exit on error

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

docker build -t rosbag2_storage_plugin_comparison .
docker run --rm --mount type=tmpfs,dst=/tmp -v $SCRIPT_DIR:/ros2_ws/rosbag2/output rosbag2_storage_plugin_comparison
ls -lh results.csv
