# Rosbag2 storage plugin comparison

This package contains a benchmark for comparing the write throughput and overall memory
usage of different storage plugins in different configurations. This can be used to determine
the storage options that best suit your use-case.

## How to use

### Building

```
colcon build --packages-up-to rosbag2_storage_plugin_comparison
```

### Running the benchmark
```
ros2 pkg run rosbag2_storage_plugin_comparison sweep.py results.csv
```

### Changing the benchmark sweep parameters

`sweep.py` runs the benchmark binary a number of times across a set of parameters. The parameters
and their values are defined in `CONFIG_DIMENSIONS` in sweep.py. Check `config.hpp` for the the set
of config parameters available to you.
