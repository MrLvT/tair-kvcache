#include "kv_cache_manager/optimizer/config/optimizer_config.h"

#include "kv_cache_manager/common/logger.h"

namespace kv_cache_manager {
bool OptComputeTimeConfig::FromRapidValue(const rapidjson::Value &rapid_value) {
    if (!rapid_value.IsObject()) {
        KVCM_LOG_ERROR("trace_replay.compute_time must be an object");
        return false;
    }
    KVCM_JSON_GET_DEFAULT_MACRO(rapid_value, "enabled", enabled_, false);
    KVCM_JSON_GET_DEFAULT_MACRO(rapid_value, "queue_by_instance_group", queue_by_instance_group_, true);
    KVCM_JSON_GET_DEFAULT_MACRO(rapid_value, "base_latency_ns", base_latency_ns_, int64_t(0));
    KVCM_JSON_GET_DEFAULT_MACRO(rapid_value, "miss_block_latency_ns", miss_block_latency_ns_, int64_t(0));
    KVCM_JSON_GET_DEFAULT_MACRO(
        rapid_value, "miss_block_position_latency_ns", miss_block_position_latency_ns_, int64_t(0));
    KVCM_JSON_GET_DEFAULT_MACRO(rapid_value, "latency_offset_ns", latency_offset_ns_, int64_t(0));
    KVCM_JSON_GET_DEFAULT_MACRO(rapid_value, "scheduler_load_divisor", scheduler_load_divisor_, 1.0);
    KVCM_JSON_GET_DEFAULT_MACRO(rapid_value, "scheduler_lane_count", scheduler_lane_count_, int32_t(1));
    KVCM_JSON_GET_DEFAULT_MACRO(
        rapid_value, "scheduler_lane_count_alternate", scheduler_lane_count_alternate_, int32_t(0));
    KVCM_JSON_GET_DEFAULT_MACRO(rapid_value, "noise_offsets_ns", noise_offsets_ns_, std::vector<int64_t>{});
    KVCM_JSON_GET_DEFAULT_MACRO(rapid_value, "noise_weights", noise_weights_, std::vector<double>{});
    KVCM_JSON_GET_DEFAULT_MACRO(rapid_value, "noise_seed", noise_seed_, uint64_t(0));

    if (miss_block_latency_ns_ < 0 || miss_block_position_latency_ns_ < 0 || latency_offset_ns_ < 0) {
        KVCM_LOG_ERROR(
            "trace_replay.compute_time miss latency, position latency and latency offset must be non-negative");
        return false;
    }
    if (scheduler_load_divisor_ <= 0.0) {
        KVCM_LOG_ERROR("trace_replay.compute_time.scheduler_load_divisor must be positive");
        return false;
    }
    if (scheduler_lane_count_ <= 0 || scheduler_lane_count_alternate_ < 0) {
        KVCM_LOG_ERROR("trace_replay.compute_time scheduler lane counts must be positive or zero alternate");
        return false;
    }
    for (auto offset : noise_offsets_ns_) {
        if (offset < 0) {
            KVCM_LOG_ERROR("trace_replay.compute_time.noise_offsets_ns must contain only non-negative values");
            return false;
        }
    }
    if (!noise_offsets_ns_.empty()) {
        if (noise_weights_.size() != noise_offsets_ns_.size()) {
            KVCM_LOG_ERROR("trace_replay.compute_time.noise_weights must have the same size as noise_offsets_ns");
            return false;
        }
        double weight_sum = 0.0;
        for (auto weight : noise_weights_) {
            if (weight < 0.0) {
                KVCM_LOG_ERROR("trace_replay.compute_time.noise_weights must contain only non-negative values");
                return false;
            }
            weight_sum += weight;
        }
        if (weight_sum <= 0.0) {
            KVCM_LOG_ERROR("trace_replay.compute_time.noise_weights must sum to a positive value");
            return false;
        }
    } else if (!noise_weights_.empty()) {
        KVCM_LOG_ERROR("trace_replay.compute_time.noise_weights requires noise_offsets_ns");
        return false;
    }

    if (enabled_ && base_latency_ns_ == 0 && miss_block_latency_ns_ == 0 &&
        miss_block_position_latency_ns_ == 0 && latency_offset_ns_ == 0 && noise_offsets_ns_.empty()) {
        KVCM_LOG_ERROR("trace_replay.compute_time is enabled but all latency parameters are zero");
        return false;
    }
    return true;
}

void OptComputeTimeConfig::ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept {
    Put(writer, "enabled", enabled_);
    Put(writer, "queue_by_instance_group", queue_by_instance_group_);
    Put(writer, "base_latency_ns", base_latency_ns_);
    Put(writer, "miss_block_latency_ns", miss_block_latency_ns_);
    Put(writer, "miss_block_position_latency_ns", miss_block_position_latency_ns_);
    Put(writer, "latency_offset_ns", latency_offset_ns_);
    Put(writer, "scheduler_load_divisor", scheduler_load_divisor_);
    Put(writer, "scheduler_lane_count", scheduler_lane_count_);
    if (scheduler_lane_count_alternate_ > 0) {
        Put(writer, "scheduler_lane_count_alternate", scheduler_lane_count_alternate_);
    }
    if (!noise_offsets_ns_.empty()) {
        Put(writer, "noise_offsets_ns", noise_offsets_ns_);
        Put(writer, "noise_weights", noise_weights_);
        Put(writer, "noise_seed", noise_seed_);
    }
}

bool OptTraceReplayConfig::FromRapidValue(const rapidjson::Value &rapid_value) {
    KVCM_JSON_GET_DEFAULT_MACRO(rapid_value, "write_delay_ns", write_delay_ns_, int64_t(1));
    if (write_delay_ns_ <= 0) {
        KVCM_LOG_ERROR("trace_replay.write_delay_ns must be positive, got %ld", write_delay_ns_);
        return false;
    }
    compute_time_config_ = OptComputeTimeConfig();
    if (rapid_value.HasMember("compute_time")) {
        if (!compute_time_config_.FromRapidValue(rapid_value["compute_time"])) {
            return false;
        }
    }
    return true;
}

void OptTraceReplayConfig::ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept {
    Put(writer, "write_delay_ns", write_delay_ns_);
    Put(writer, "compute_time", compute_time_config_);
}

bool OptimizerConfig::FromRapidValue(const rapidjson::Value &rapid_value) {
    KVCM_JSON_GET_MACRO(rapid_value, "trace_file_path", trace_file_path_);
    KVCM_JSON_GET_MACRO(rapid_value, "output_result_path", output_result_path_);
    KVCM_JSON_GET_MACRO(rapid_value, "eviction_params", eviction_config_);
    trace_replay_config_ = OptTraceReplayConfig();
    if (rapid_value.HasMember("trace_replay")) {
        if (!rapid_value["trace_replay"].IsObject()) {
            KVCM_LOG_ERROR("trace_replay must be an object");
            return false;
        }
        if (!trace_replay_config_.FromRapidValue(rapid_value["trace_replay"])) {
            return false;
        }
    }
    KVCM_JSON_GET_MACRO(rapid_value, "instance_groups", instance_groups_);
    return true;
};

void OptimizerConfig::ToRapidWriter(rapidjson::Writer<rapidjson::StringBuffer> &writer) const noexcept {
    Put(writer, "trace_file_path", trace_file_path_);
    Put(writer, "output_result_path", output_result_path_);
    Put(writer, "eviction_params", eviction_config_);
    Put(writer, "trace_replay", trace_replay_config_);
    Put(writer, "instance_groups", instance_groups_);
}
} // namespace kv_cache_manager
