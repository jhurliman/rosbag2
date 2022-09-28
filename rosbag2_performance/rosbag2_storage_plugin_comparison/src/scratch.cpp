#include <malloc.h>
#include <vector>
#include <iostream>
#include <unordered_map>

#include "rosbag2_cpp/writer.hpp"

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


using hrc = std::chrono::high_resolution_clock;
using Batch = std::vector<std::shared_ptr<const rosbag2_storage::SerializedBagMessage>>;

struct Config {
    std::string storage_id = "";
    size_t batch_message_count = 100;

    std::unordered_map<std::string, std::string> plugin_config;
};


std::vector<rosbag2_storage::TopicMetadata> generate_topics() {
    std::vector<rosbag2_storage::TopicMetadata> topics;
    return topics;
}

std::vector<Batch> generate_messages() {
    std::vector<std::vector<std::shared_ptr<const rosbag2_storage::SerializedBagMessage>>> out;
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


int main(int argc, const char* argv[]) {
    if (argc < 2) {
        std::cerr << "please supply an options file as input" << std::endl;
        return 1;
    }
    auto topics = generate_topics();
    auto messages = generate_messages();

    rosbag2_storage::StorageFactory factory;
    rosbag2_storage::StorageOptions options;
    options.uri = "out.mcap";
    options.storage_id = "mcap";

    std::vector<WriteStat> writeStats;
    writeStats.reserve(messages.size());
    std::chrono::high_resolution_clock::time_point close_start_time;
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
    return 0;
}
