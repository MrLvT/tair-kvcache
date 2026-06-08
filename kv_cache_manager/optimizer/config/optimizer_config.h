#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kv_cache_manager/common/jsonizable.h"
#include "kv_cache_manager/optimizer/config/eviction_config.h"
#include "kv_cache_manager/optimizer/config/instance_group_config.h"
#include "kv_cache_manager/optimizer/config/types.h"
namespace kv_cache_manager {

class OptComputeTimeConfig : public Jsonizable {
public:
    OptComputeTimeConfig() = default;
    ~OptComputeTimeConfig() override = default;
    bool FromRapidValue(const rapidjson::Value &rapid_value) override;
    void ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept override;

    [[nodiscard]] bool enabled() const { return enabled_; }
    [[nodiscard]] bool queue_by_instance_group() const { return queue_by_instance_group_; }
    [[nodiscard]] int64_t base_latency_ns() const { return base_latency_ns_; }
    [[nodiscard]] int64_t miss_block_latency_ns() const { return miss_block_latency_ns_; }
    [[nodiscard]] int64_t miss_block_position_latency_ns() const { return miss_block_position_latency_ns_; }
    [[nodiscard]] int64_t latency_offset_ns() const { return latency_offset_ns_; }
    [[nodiscard]] double scheduler_load_divisor() const { return scheduler_load_divisor_; }
    [[nodiscard]] int32_t scheduler_lane_count() const { return scheduler_lane_count_; }
    [[nodiscard]] int32_t scheduler_lane_count_alternate() const { return scheduler_lane_count_alternate_; }
    [[nodiscard]] const std::vector<int64_t> &noise_offsets_ns() const { return noise_offsets_ns_; }
    [[nodiscard]] const std::vector<double> &noise_weights() const { return noise_weights_; }
    [[nodiscard]] uint64_t noise_seed() const { return noise_seed_; }
    void set_enabled(bool enabled) { enabled_ = enabled; }
    void set_queue_by_instance_group(bool enabled) { queue_by_instance_group_ = enabled; }
    void set_base_latency_ns(int64_t value) { base_latency_ns_ = value; }
    void set_miss_block_latency_ns(int64_t value) { miss_block_latency_ns_ = value; }
    void set_miss_block_position_latency_ns(int64_t value) { miss_block_position_latency_ns_ = value; }
    void set_latency_offset_ns(int64_t value) { latency_offset_ns_ = value; }
    void set_scheduler_load_divisor(double value) { scheduler_load_divisor_ = value; }
    void set_scheduler_lane_count(int32_t value) { scheduler_lane_count_ = value; }
    void set_scheduler_lane_count_alternate(int32_t value) { scheduler_lane_count_alternate_ = value; }
    void set_noise_offsets_ns(const std::vector<int64_t> &values) { noise_offsets_ns_ = values; }
    void set_noise_weights(const std::vector<double> &values) { noise_weights_ = values; }
    void set_noise_seed(uint64_t value) { noise_seed_ = value; }

private:
    bool enabled_ = false;
    bool queue_by_instance_group_ = true;
    int64_t base_latency_ns_ = 0;
    int64_t miss_block_latency_ns_ = 0;
    int64_t miss_block_position_latency_ns_ = 0;
    int64_t latency_offset_ns_ = 0;
    double scheduler_load_divisor_ = 1.0;
    int32_t scheduler_lane_count_ = 1;
    int32_t scheduler_lane_count_alternate_ = 0;
    std::vector<int64_t> noise_offsets_ns_;
    std::vector<double> noise_weights_;
    uint64_t noise_seed_ = 0;
};

class OptTraceReplayConfig : public Jsonizable {
public:
    OptTraceReplayConfig() = default;
    ~OptTraceReplayConfig() override = default;
    bool FromRapidValue(const rapidjson::Value &rapid_value) override;
    void ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept override;

    [[nodiscard]] int64_t write_delay_ns() const { return write_delay_ns_; }
    [[nodiscard]] const OptComputeTimeConfig &compute_time_config() const { return compute_time_config_; }
    void set_write_delay_ns(int64_t delay_ns) { write_delay_ns_ = delay_ns; }
    void set_compute_time_config(const OptComputeTimeConfig &config) { compute_time_config_ = config; }

private:
    int64_t write_delay_ns_ = 1;
    OptComputeTimeConfig compute_time_config_;
};

class OptimizerConfig : public Jsonizable {
public:
    OptimizerConfig() = default;
    ~OptimizerConfig() override = default;
    bool FromRapidValue(const rapidjson::Value &rapid_value) override;
    void ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept override;

public:
    [[nodiscard]] const std::string &trace_file_path() const { return trace_file_path_; }
    [[nodiscard]] const std::string &output_result_path() const { return output_result_path_; }
    [[nodiscard]] const EvictionConfig &eviction_config() const { return eviction_config_; }
    [[nodiscard]] const OptTraceReplayConfig &trace_replay_config() const { return trace_replay_config_; }
    [[nodiscard]] const std::vector<OptInstanceGroupConfig> &instance_groups() const { return instance_groups_; }
    [[nodiscard]] std::vector<OptInstanceGroupConfig> &mutable_instance_groups() { return instance_groups_; }

    void set_trace_file_path(const std::string &path) { trace_file_path_ = path; }
    void set_output_result_path(const std::string &path) { output_result_path_ = path; }
    void set_eviction_params(const EvictionConfig &config) { eviction_config_ = config; }
    void set_trace_replay_config(const OptTraceReplayConfig &config) { trace_replay_config_ = config; }
    void set_instance_groups(const std::vector<OptInstanceGroupConfig> &groups) { instance_groups_ = groups; }

private:
    std::string trace_file_path_;
    std::string output_result_path_;
    EvictionConfig eviction_config_;
    OptTraceReplayConfig trace_replay_config_;
    std::vector<OptInstanceGroupConfig> instance_groups_;
};

} // namespace kv_cache_manager
