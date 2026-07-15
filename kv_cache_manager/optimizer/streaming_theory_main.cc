#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <stdexcept>
#include <string>
#include <vector>

#include "rapidjson/document.h"

#include "kv_cache_manager/optimizer/config/optimizer_config_loader.h"
#include "kv_cache_manager/optimizer/eviction_policy/lru.h"
#include "kv_cache_manager/optimizer/index/radix_tree_index.h"

namespace kv_cache_manager {
namespace {

struct PendingWrite {
    int64_t timestamp_ns = 0;
    uint64_t sequence = 0;
    std::vector<int64_t> keys;
};

struct PendingWriteCompare {
    bool operator()(const PendingWrite &lhs, const PendingWrite &rhs) const {
        if (lhs.timestamp_ns != rhs.timestamp_ns) {
            return lhs.timestamp_ns > rhs.timestamp_ns;
        }
        return lhs.sequence > rhs.sequence;
    }
};

struct RequestRow {
    std::string trace_id;
    int64_t timestamp_ns = 0;
    int64_t input_len = 0;
    std::vector<int64_t> keys;
};

int64_t ParseI64(const rapidjson::Value &value, const char *field) {
    int64_t parsed = 0;
    if (!ParseOptimizerInt64(value, parsed)) {
        throw std::runtime_error(std::string("invalid int64 field: ") + field);
    }
    return parsed;
}

RequestRow ParseRequestLine(const std::string &line) {
    rapidjson::Document doc;
    if (doc.Parse(line.c_str()).HasParseError() || !doc.IsObject()) {
        throw std::runtime_error("failed to parse request json");
    }
    if (!doc.HasMember("type") || !doc["type"].IsString() || std::string(doc["type"].GetString()) != "request") {
        throw std::runtime_error("streaming theory only supports request traces");
    }
    RequestRow row;
    if (doc.HasMember("trace_id") && doc["trace_id"].IsString()) {
        row.trace_id = doc["trace_id"].GetString();
    }
    if (!doc.HasMember("timestamp_ns")) {
        throw std::runtime_error("missing timestamp_ns");
    }
    row.timestamp_ns = ParseI64(doc["timestamp_ns"], "timestamp_ns");
    if (!doc.HasMember("input_len")) {
        throw std::runtime_error("missing input_len");
    }
    row.input_len = ParseI64(doc["input_len"], "input_len");
    if (!ParseOptimizerKeyVector(doc, "keys", row.keys)) {
        throw std::runtime_error("missing or invalid keys");
    }
    return row;
}

const OptInstanceConfig &SingleInstance(const OptimizerConfig &config) {
    const auto &groups = config.instance_groups();
    if (groups.size() != 1 || groups[0].instances().size() != 1) {
        throw std::runtime_error("streaming theory expects exactly one instance");
    }
    return groups[0].instances()[0];
}

void FlushPendingWrites(std::priority_queue<PendingWrite, std::vector<PendingWrite>, PendingWriteCompare> *pending,
                        RadixTreeIndex *index,
                        int64_t timestamp_ns) {
    while (!pending->empty() && pending->top().timestamp_ns <= timestamp_ns) {
        PendingWrite write = std::move(const_cast<PendingWrite &>(pending->top()));
        pending->pop();
        index->InsertOnly(write.keys, write.timestamp_ns, -1);
    }
}

std::string JsonEscape(const std::string &value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (char ch : value) {
        switch (ch) {
        case '\\':
            out += "\\\\";
            break;
        case '"':
            out += "\\\"";
            break;
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        case '\t':
            out += "\\t";
            break;
        default:
            out.push_back(ch);
            break;
        }
    }
    return out;
}

void WriteWarmupJson(const std::string &path,
                     const std::string &instance_id,
                     double hit_rate,
                     uint64_t max_blocks,
                     int64_t block_size,
                     int64_t bytes_per_token,
                     uint64_t request_count,
                     uint64_t input_tokens,
                     uint64_t hit_tokens) {
    std::filesystem::create_directories(std::filesystem::path(path).parent_path());
    const double cached_gb = static_cast<double>(max_blocks) * static_cast<double>(block_size) *
                             static_cast<double>(bytes_per_token) / 1e9;
    std::ofstream out(path);
    if (!out.is_open()) {
        throw std::runtime_error("failed to open warmup json: " + path);
    }
    out << std::setprecision(12);
    out << "{\n"
        << "  \"policies\": {\n"
        << "    \"default_policy\": {\n"
        << "      \"instances\": {\n"
        << "        \"" << JsonEscape(instance_id) << "\": {\n"
        << "          \"total\": " << hit_rate << ",\n"
        << "          \"cached_gb\": " << cached_gb << ",\n"
        << "          \"request_count\": " << request_count << ",\n"
        << "          \"input_tokens\": " << input_tokens << ",\n"
        << "          \"hit_tokens\": " << hit_tokens << "\n"
        << "        }\n"
        << "      },\n"
        << "      \"max_blocks\": " << max_blocks << "\n"
        << "    }\n"
        << "  }\n"
        << "}\n";
}

int RunStreamingTheory(const std::string &config_path, const std::string &warmup_json) {
    OptimizerConfigLoader loader;
    if (!loader.Load(config_path)) {
        throw std::runtime_error("failed to load optimizer config: " + config_path);
    }
    OptimizerConfig config = loader.get_config();
    const OptInstanceConfig &instance = SingleInstance(config);
    LruParams params;
    params.sample_rate = 1.0;
    params.shard_count = 1;
    params.sample_times = 32;
    params.eviction_amplification_factor = 1.0;
    auto policy = std::make_shared<LruEvictionPolicy>("shared", params);
    RadixTreeIndex index(instance.instance_id(), policy, -1);

    std::ifstream input(config.trace_file_path());
    if (!input.is_open()) {
        throw std::runtime_error("failed to open trace: " + config.trace_file_path());
    }

    std::priority_queue<PendingWrite, std::vector<PendingWrite>, PendingWriteCompare> pending;
    std::string line;
    uint64_t sequence = 0;
    uint64_t request_count = 0;
    uint64_t input_tokens = 0;
    uint64_t hit_tokens = 0;
    const int64_t block_size = instance.block_size();
    const int64_t bytes_per_token = instance.bytes_per_token();
    const int64_t write_delay_ns = config.trace_replay_config().write_delay_ns();

    while (std::getline(input, line)) {
        if (line.find_first_not_of(" \t\r\n") == std::string::npos) {
            continue;
        }
        RequestRow row = ParseRequestLine(line);
        FlushPendingWrites(&pending, &index, row.timestamp_ns);
        const size_t hit_blocks = index.PrefixMatchCount(row.keys, row.timestamp_ns);
        hit_tokens += static_cast<uint64_t>(hit_blocks) * static_cast<uint64_t>(block_size);
        input_tokens += static_cast<uint64_t>(std::max<int64_t>(row.input_len, 0));
        request_count += 1;
        pending.push(PendingWrite{row.timestamp_ns + write_delay_ns, sequence++, std::move(row.keys)});
        if (request_count % 1000000 == 0) {
            std::cerr << "stream progress requests=" << request_count << " hit_rate="
                      << (input_tokens > 0 ? static_cast<double>(hit_tokens) / static_cast<double>(input_tokens) : 0.0)
                      << " cached_blocks=" << policy->size() << "\n";
        }
    }

    const double hit_rate =
        input_tokens > 0 ? static_cast<double>(hit_tokens) / static_cast<double>(input_tokens) : 0.0;
    WriteWarmupJson(warmup_json,
                    instance.instance_id(),
                    hit_rate,
                    static_cast<uint64_t>(policy->size()),
                    block_size,
                    bytes_per_token,
                    request_count,
                    input_tokens,
                    hit_tokens);
    std::cerr << "stream done requests=" << request_count << " hit_rate=" << hit_rate
              << " cached_blocks=" << policy->size() << " warmup_json=" << warmup_json << "\n";
    return 0;
}

} // namespace
} // namespace kv_cache_manager

int main(int argc, char **argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <optimizer_config.json> <warmup_result.json>\n";
        return 2;
    }
    try {
        return kv_cache_manager::RunStreamingTheory(argv[1], argv[2]);
    } catch (const std::exception &ex) {
        std::cerr << "streaming theory failed: " << ex.what() << "\n";
        return 1;
    }
}
