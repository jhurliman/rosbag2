#include <malloc.h>
#include <random>
#include <fstream>
#include <climits>
#include <algorithm>
#include <functional>
#include <vector>
#include <iostream>
#include <unordered_map>

#include "rosbag2_cpp/writer.hpp"
#include "rosbag2_storage/ros_helper.hpp"
#include "rcutils/logging_macros.h"

#include "yaml-cpp/yaml.h"

using RandomEngine = std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char>;
using hrc = std::chrono::high_resolution_clock;
using Batch = std::vector<std::shared_ptr<const rosbag2_storage::SerializedBagMessage>>;

struct Config {
    std::string storage_id = "sqlite3";
    size_t min_batch_size_bytes = 10;
    size_t write_total_bytes = 1000000000;

    struct TopicConfig {
        std::string name;
        size_t message_size;
        double write_proportion = 1.0;
    };
    std::vector<TopicConfig> topics = { {"/large", 1000000, 0.8 }, {"/small", 1000, 0.1 } };

    YAML::Node storage_options;
};

namespace YAML {
template<>
struct convert<Config> {
  static Node encode(const Config& rhs) {
    Node node;
    node["storage_id"] = rhs.storage_id;
    node["min_batch_size_bytes"] = rhs.min_batch_size_bytes;
    node["write_total_bytes"] = rhs.write_total_bytes;
    Node topic_configs_node;
    for (const auto& topic_config : rhs.topics) {
        Node topic_config_node;
        topic_config_node["name"] = topic_config.name;
        topic_config_node["message_size"] = topic_config.message_size;
        topic_config_node["write_proportion"] = topic_config.write_proportion;
        topic_configs_node.push_back(topic_config_node);
    }
    node["topics"] = topic_configs_node;
    node["storage_options"] = rhs.storage_options;
    return node;
  }

  static bool decode(const Node& node, Config& rhs) {
    optional_assign<std::string>(node, "storage_id", rhs.storage_id);
    optional_assign<size_t>(node, "min_batch_size_bytes", rhs.min_batch_size_bytes);
    optional_assign<size_t>(node, "write_total_bytes", rhs.write_total_bytes);
    if (node["topics"]) {
        rhs.topics.clear();
        auto topics_node = node["topics"];
        for (auto it = topics_node.begin(); it != topics_node.end(); ++it) {
            Config::TopicConfig topic;
            topic.name = (*it)["name"].as<std::string>();
            topic.message_size = (*it)["message_size"].as<size_t>();
            optional_assign<double>(*it, "write_proportion", topic.write_proportion);
            rhs.topics.push_back(topic);
        }
    }
    optional_assign<Node>(node, "storage_options", rhs.storage_options);
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
    size_t currentBatchBytes = 0;
    RandomEngine engine;
    std::default_random_engine shuffleEngine(0);
    // TODO: check that the proportions sum to roughly one

    std::vector<const Config::TopicConfig*> config_to_write;
    for (size_t i = 0; i < config.topics.size(); ++i) {
        const auto* topic_config = &config.topics[i];
        double this_topic_bytes = (double)(config.write_total_bytes) * topic_config->write_proportion;
        size_t num_msgs = (size_t)(this_topic_bytes / double(topic_config->message_size));
        for (size_t j = 0; j < num_msgs; ++j) {
            config_to_write.push_back(topic_config);
        }
    }
    // shuffle the vector
    std::shuffle(config_to_write.begin(), config_to_write.end(), shuffleEngine);

    for (auto topic_config: config_to_write) {
        auto msg = std::make_shared<rosbag2_storage::SerializedBagMessage>();
        msg->topic_name = topic_config->name;
        msg->serialized_data = random_uint8_array(topic_config->message_size, engine);
        currentBatchBytes += topic_config->message_size;
        currentBatch.push_back(msg);
        if (currentBatchBytes >= config.min_batch_size_bytes) {
            out.emplace_back(std::move(currentBatch));
            currentBatch = {};
            currentBatchBytes = 0;
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


int main(int argc, const char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <config yaml string> <output dir>" << std::endl;
        std::cerr << "Use ros2 run rosbag2_storage_plugin_comparison sweep.py for a more ergonomic experience" << std::endl;
        return 1;
    }
    YAML::Node config_yaml = YAML::Load(argv[1]);
    Config config = config_yaml.as<Config>();
    RCUTILS_LOG_INFO_NAMED("single_benchmark", "generating %ld topics", config.topics.size());
    auto topics = generate_topics(config);
    RCUTILS_LOG_INFO_NAMED("single_benchmark", "generating some messages");
    auto messages = generate_messages(config);
    RCUTILS_LOG_INFO_NAMED("single_benchmark", "configuring writer");
    rosbag2_storage::StorageFactory factory;
    rosbag2_storage::StorageOptions options;
    options.uri = std::string(argv[2]) + "/out";
    options.storage_id = config.storage_id;

    if (config.storage_options.IsMap()) {
        std::string storage_options_uri = std::string(argv[2]) + "/storage_options.yaml";
        RCUTILS_LOG_INFO_NAMED("single_benchmark", "using storage options %s", storage_options_uri.c_str());
        std::ofstream fout(storage_options_uri.c_str());
        fout << config.storage_options;
        options.storage_config_uri = storage_options_uri;
    }

    std::vector<WriteStat> writeStats;
    writeStats.reserve(messages.size());
    std::chrono::high_resolution_clock::time_point close_start_time;
    RCUTILS_LOG_INFO_NAMED("single_benchmark", "writing messages");
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
