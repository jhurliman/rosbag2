FROM ros:humble-ros-core

RUN apt-get update && \
  apt-get install -y --no-install-recommends --no-install-suggests \
  build-essential \
  ros-humble-ros-base \
  ros-humble-ros2bag \
  ros-humble-rosbag2-transport \
  ros-humble-rosbag2-storage-mcap \
  ros-humble-test-msgs \
  python3 \
  python3-pip \
  python3-colcon-common-extensions \
  git

WORKDIR /ros2_ws

ADD https://api.github.com/repos/jhurliman/rosbag2/git/refs/heads/jrms/plugin-comparison version.json
RUN git clone -b jrms/plugin-comparison https://github.com/jhurliman/rosbag2.git
WORKDIR /ros2_ws/rosbag2

SHELL ["/bin/bash", "-c"]
RUN source /opt/ros/humble/setup.bash && colcon build --packages-up-to rosbag2_storage_plugin_comparison

COPY _container_entrypoint.sh /ros2_ws/rosbag2/_container_entrypoint.sh

CMD ["/ros2_ws/rosbag2/_container_entrypoint.sh"]
