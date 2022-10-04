#!/usr/bin/env bash

set -e # Exit on error

source /opt/ros/humble/setup.bash
source ./install/local_setup.bash
ros2 run rosbag2_storage_plugin_comparison sweep.py output/results.csv
