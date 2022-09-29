#include <malloc.h>
#include <random>
#include <climits>
#include <algorithm>
#include <functional>
#include <vector>
#include <iostream>
#include <unordered_map>

#include "rosbag2_cpp/writer.hpp"
#include "rosbag2_storage/ros_helper.hpp"

#ifdef _WIN32
// This is necessary because of a bug in yaml-cpp's cmake
#define YAML_CPP_DLL
// This is necessary because yaml-cpp does not always use dllimport/dllexport consistently
# pragma warning(push)
# pragma warning(disable:4251)
# pragma warning(disable:4275)
#endif
#include "yaml-cpp/yaml.h"
#ifdef _WIN32
# pragma warning(pop)
#endif

using RandomEngine = std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char>;
using hrc = std::chrono::high_resolution_clock;
using Batch = std::vector<std::shared_ptr<const rosbag2_storage::SerializedBagMessage>>;

struct Config {
    std::string storage_id = "mcap";
    size_t batch_num_messages = 10;
    size_t repeat_message_count = 1000;

    struct TopicConfig {
        std::string name;
        size_t message_size;
    };
    std::vector<TopicConfig> topics = { {"/large", 1000000 }, {"/small", 1000} };

    std::unordered_map<std::string, std::string> plugin_config;
};

namespace YAML {
template<>
struct convert<Config> {
  static Node encode(const Config& rhs) {
    Node node;
    node["storage_id"] = rhs.storage_id;
    node["batch_num_messages"] = rhs.batch_num_messages;
    node["repeat_message_count"] = rhs.repeat_message_count;
    Node topic_configs_node;
    for (const auto& topic_config : rhs.topics) {
        Node topic_config_node;
        topic_config_node["name"] = topic_config.name;
        topic_config_node["message_size"] = topic_config.message_size;
        topic_configs_node.push_back(topic_config_node);
    }
    node["topics"] = topic_configs_node;
    Node plugin_config_node;
    for (const auto& [k, v] : rhs.plugin_config) {
        plugin_config_node[k] = v;
    }
    node["plugin_config"] = plugin_config_node;
    return node;
  }

  static bool decode(const Node& node, Config& rhs) {
    optional_assign<std::string>(node, "storage_id", rhs.storage_id);
    optional_assign<size_t>(node, "batch_num_messages", rhs.batch_num_messages);
    optional_assign<size_t>(node, "repeat_message_count", rhs.repeat_message_count);
    if (node["topics"]) {
        rhs.topics.clear();
        auto topics_node = node["topics"];
        for (auto it = topics_node.begin(); it != topics_node.end(); ++it) {
            Config::TopicConfig topic;
            topic.name = (*it)["name"].as<std::string>();
            topic.message_size = (*it)["message_size"].as<size_t>();
            rhs.topics.push_back(topic);
        }
    }
    optional_assign<std::unordered_map<std::string, std::string>>(node, "plugin_config", rhs.plugin_config);
    return true;
  }
};
}


std::vector<rosbag2_storage::TopicMetadata> generate_topics(const Config& config) {
    std::vector<rosbag2_storage::TopicMetadata> topics;
    for (const auto& topic : config.topics) {
        rosbag2_storage::TopicMetadata metadata;
        metadata.name = topic.name;
        // The topic type doesn't matter here - we're not doing any serialization,
        // just throwing random bytes into the serialized message.
        metadata.type = "std_msgs/String";
        metadata.serialization_format = "cdr";
        metadata.offered_qos_profiles = "";
        topics.push_back(metadata);
    }
    return topics;
}

std::shared_ptr<rcutils_uint8_array_t> random_uint8_array(size_t size, RandomEngine& engine) {
    std::vector<unsigned char> data(size);
    std::generate(begin(data), end(data), std::ref(engine));
    return rosbag2_storage::make_serialized_message(data.data(), data.size());
}

std::vector<Batch> generate_messages(Config config) {
    std::vector<Batch> out;
    Batch currentBatch;
    RandomEngine engine;
    for (size_t i = 0; i < config.repeat_message_count; ++i) {
        for (const auto& topic_config: config.topics) {
            auto msg = std::make_shared<rosbag2_storage::SerializedBagMessage>();
            msg->topic_name = topic_config.name;
            msg->serialized_data = random_uint8_array(topic_config.message_size, engine);
            currentBatch.push_back(msg);
            if (currentBatch.size() >= config.batch_num_messages) {
                out.emplace_back(std::move(currentBatch));
                currentBatch = {};
            }
        }
    }
    if (currentBatch.size() != 0) {
        out.emplace_back(std::move(currentBatch));
    }
    return out;
}

size_t message_bytes(const Batch& batch) {
    size_t total = 0;
    for (const auto& msgPtr : batch) {
        total += msgPtr->serialized_data->buffer_length;
    }
    return total;
}

struct BaselineStat {
    size_t arenaBytes;
    size_t inUseBytes;
    size_t mmapBytes;

    BaselineStat() {
        struct mallinfo2 info = mallinfo2();
        arenaBytes = info.arena;
        inUseBytes = info.uordblks;
        mmapBytes = info.hblkhd;
    }
};

struct WriteStat {
    uint32_t sqc;
    size_t bytesWritten;
    size_t numMsgs;
    hrc::duration writeDuration;
    ssize_t arenaBytes;
    ssize_t inUseBytes;
    ssize_t mmapBytes;

    WriteStat(const BaselineStat& baselineStat, uint32_t sqc_, const Batch& batch, hrc::duration writeDuration) :
        sqc(sqc_),
        bytesWritten(message_bytes(batch)),
        numMsgs(batch.size()),
        writeDuration(writeDuration)
        {
            struct mallinfo2 info = mallinfo2();
            arenaBytes = info.arena - baselineStat.arenaBytes;
            inUseBytes = info.uordblks - baselineStat.inUseBytes;
            mmapBytes = info.hblkhd - baselineStat.mmapBytes;
        }
};


int main(int, const char**) {
    Config config;
    std::cerr << "generating topics" << std::endl;
    auto topics = generate_topics(config);
    std::cerr << "generating " << config.repeat_message_count * config.topics.size() << " messages" << std::endl;
    auto messages = generate_messages(config);

    std::cerr << "setting up writer" << std::endl;
    rosbag2_storage::StorageFactory factory;
    rosbag2_storage::StorageOptions options;
    options.uri = "out";
    options.storage_id = config.storage_id;

    std::vector<WriteStat> writeStats;
    writeStats.reserve(messages.size());
    std::chrono::high_resolution_clock::time_point close_start_time;
    std::cerr << "starting writes" << std::endl;
    {
        auto writer = factory.open_read_write(options);

        for (const auto& topic : topics) {
            writer->create_topic(topic);
        }

        BaselineStat baseline;
        uint32_t sqc = 0;
        // write messages, timing each write
        for (const auto& message_batch : messages) {
            auto start_time = hrc::now();
            writer->write(message_batch);
            hrc::duration duration = hrc::now() - start_time;
            writeStats.emplace_back(baseline, sqc, message_batch, duration);
            sqc++;
        }

        close_start_time = hrc::now();
        // writer destructor closes the output file, so we time that too.
    }
    auto close_duration = hrc::now() - close_start_time;

    std::cout << "sqc,num_bytes,num_msgs,write_ns,arena_bytes,in_use_bytes,mmap_bytes,close_ns" << std::endl;
    for (const auto& stat : writeStats) {
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stat.writeDuration).count();
        std::cout <<
            stat.sqc << "," <<
            stat.bytesWritten << "," <<
            stat.numMsgs << "," <<
            duration << "," <<
            stat.arenaBytes << "," <<
            stat.inUseBytes << "," <<
            stat.mmapBytes << "," <<
            std::endl;
    }
    std::cout << ",,,,,,," << std::chrono::duration_cast<std::chrono::nanoseconds>(close_duration).count() << std::endl;
    return 0;
}
