#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "kv_cache_manager/optimizer/analysis/tracker/cache_retention_tracker.h"
#include "kv_cache_manager/optimizer/config/optimizer_config.h"

namespace kv_cache_manager {

class CacheCapacityAutoscaler {
public:
    static constexpr int64_t kSecondNs = 1000LL * 1000 * 1000;
    static constexpr int64_t kTiB = 1024LL * 1024 * 1024 * 1024;

    struct ApplyResult {
        bool applied = false;
        int64_t capacity_before_bytes = 0;
        int64_t capacity_after_bytes = 0;
        std::string reason;
    };

    using ApplyCallback = std::function<ApplyResult(int64_t delta_bytes, int64_t effective_time_ns)>;

    CacheCapacityAutoscaler(OptCacheAutoscalingConfig config,
                            std::string target_group,
                            int64_t baseline_capacity_bytes);

    void OnMetric(const CacheRetentionTracker::SummaryRow &row, int64_t decision_time_ns);
    void ApplyDueThrough(int64_t timestamp_ns, const ApplyCallback &apply);
    void ExportCsv(const std::string &output_result_path) const;

    bool has_pending_action() const { return pending_event_index_.has_value(); }
    std::optional<int64_t> pending_effective_time_ns() const;
    int64_t current_capacity_bytes() const { return current_capacity_bytes_; }
    int64_t baseline_capacity_bytes() const { return baseline_capacity_bytes_; }
    int64_t extra_units() const { return extra_units_; }

private:
    enum class Zone { UNKNOWN, LOW, DEAD_BAND, HIGH };

    struct Event {
        size_t event_id = 0;
        int64_t metric_minute_start_ns = 0;
        double metric_value_seconds = 0.0;
        size_t evicted_blocks = 0;
        std::string action;
        int64_t trigger_time_ns = 0;
        int64_t scheduled_effective_time_ns = 0;
        std::optional<int64_t> actual_effective_time_ns;
        int64_t delta_capacity_bytes = 0;
        int64_t capacity_before_bytes = 0;
        int64_t capacity_after_bytes = 0;
        std::string status = "SCHEDULED";
        std::string reason;
    };

    void Schedule(const CacheRetentionTracker::SummaryRow &row,
                  double metric_seconds,
                  int64_t trigger_time_ns,
                  int64_t effective_time_ns,
                  int64_t delta_capacity_bytes,
                  const std::string &action,
                  const std::string &reason);

    OptCacheAutoscalingConfig config_;
    std::string target_group_;
    int64_t baseline_capacity_bytes_ = 0;
    int64_t current_capacity_bytes_ = 0;
    int64_t extra_units_ = 0;
    Zone previous_zone_ = Zone::UNKNOWN;
    std::vector<Event> events_;
    std::optional<size_t> pending_event_index_;
};

} // namespace kv_cache_manager
