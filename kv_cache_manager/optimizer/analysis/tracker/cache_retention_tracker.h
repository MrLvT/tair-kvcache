#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kv_cache_manager/optimizer/analysis/stats_tracker.h"

namespace kv_cache_manager {

class CacheRetentionTracker : public StatsTracker {
public:
    static constexpr int64_t kMinuteNs = 60LL * 1000 * 1000 * 1000;

    struct SummaryRow {
        int64_t minute_start_ns = 0;
        size_t evicted_blocks = 0;
        size_t reused_evicted_blocks = 0;
        size_t never_reused_evicted_blocks = 0;
        std::optional<double> lifetime_average_ns;
        std::optional<double> lifetime_p10_ns;
        std::optional<double> lifetime_p50_ns;
        std::optional<double> lifetime_p75_ns;
        std::optional<double> lifetime_p95_ns;
        std::optional<double> lifetime_p99_ns;
        std::optional<double> idle_average_ns;
        std::optional<double> idle_p10_ns;
        std::optional<double> idle_p50_ns;
        std::optional<double> idle_p75_ns;
        std::optional<double> idle_p95_ns;
        std::optional<double> idle_p99_ns;
        std::optional<double> lru_time_span_ns;
        std::optional<double> all_block_last_touch_age_average_ns;
        std::optional<double> all_block_last_touch_age_p10_ns;
        std::optional<double> all_block_last_touch_age_p50_ns;
        std::optional<double> all_block_last_touch_age_p95_ns;
        size_t distinct_eviction_timestamps = 0;
        size_t distinct_last_read_timestamps = 0;
        size_t largest_last_read_cohort_blocks = 0;
        std::optional<double> largest_last_read_cohort_fraction;
        size_t reuse_eviction_batches = 0;
        std::optional<double> batch_idle_spread_average_ns;
        std::optional<double> batch_idle_spread_p50_ns;
        std::optional<double> batch_idle_spread_p95_ns;
    };

    explicit CacheRetentionTracker(std::unordered_map<std::string, std::string> instance_to_service = {});

    void OnBlockBirth(const std::string &instance_id, BlockEntry *block, int64_t timestamp) override;
    void OnBlockReadHit(const std::string &instance_id, BlockEntry *block, int64_t timestamp) override;
    void OnBlockRetentionEviction(const std::string &instance_id, BlockEntry *block, int64_t timestamp) override;
    void OnTimestamp(const std::string &instance_id, int64_t timestamp) override;
    void Finalize(const std::string &instance_id, int64_t final_timestamp) override;
    void Export(const std::string &instance_id, const OptimizerConfig &config) override;
    void Reset(const std::string &instance_id) override;

    const std::vector<SummaryRow> &InstanceRows(const std::string &instance_id) const;
    const std::vector<SummaryRow> &ServiceRows(const std::string &service_name) const;
    std::vector<SummaryRow> TakeClosedServiceRowsThrough(const std::string &service_name, int64_t timestamp);
    void OnLruTimeSpanSnapshot(const std::string &instance_id,
                               int64_t minute_start_ns,
                               const std::optional<int64_t> &span_ns);

private:
    struct DiagnosticEvent {
        std::string event_type;
        std::string instance_id;
        int64_t block_key = 0;
        int64_t event_timestamp_ns = 0;
        int64_t birth_time_ns = 0;
        std::optional<int64_t> last_read_hit_time_ns;
        std::optional<int64_t> eviction_time_ns;
    };

    struct ActiveLifecycle {
        int64_t birth_time_ns = 0;
        std::optional<int64_t> last_read_hit_time_ns;
    };

    struct Bucket {
        size_t evicted_blocks = 0;
        size_t reused_evicted_blocks = 0;
        std::vector<int64_t> lifetimes_ns;
        std::vector<int64_t> idle_after_reuse_ns;
        std::optional<int64_t> lru_time_span_ns;
        std::vector<int64_t> all_block_last_touch_ages_ns;
        std::unordered_set<int64_t> eviction_timestamps_ns;
        std::unordered_map<int64_t, size_t> last_read_timestamp_counts;
        std::unordered_map<int64_t, std::vector<int64_t>> eviction_batch_idle_ns;
    };

    struct Series {
        std::optional<int64_t> observation_start_minute_ns;
        std::optional<int64_t> observation_end_minute_ns;
        std::optional<int64_t> current_minute_ns;
        Bucket current_bucket;
        std::vector<SummaryRow> rows;
        bool finalized = false;
    };

    static int64_t MinuteStart(int64_t timestamp);
    static SummaryRow Summarize(int64_t minute_start_ns, Bucket bucket);
    static void AddSample(Series *series,
                          int64_t eviction_timestamp,
                          int64_t lifetime_ns,
                          int64_t all_block_last_touch_age_ns,
                          const std::optional<int64_t> &idle_after_reuse_ns,
                          const std::optional<int64_t> &last_read_timestamp_ns);
    static void ObserveTimestamp(Series *series, int64_t timestamp);
    static void FlushThrough(Series *series, int64_t final_timestamp);
    static void FlushClosedThrough(Series *series, int64_t timestamp);
    static void WriteCsv(const std::string &filename, const std::vector<SummaryRow> &rows);
    static void WriteDiagnosticCsv(const std::string &filename, const std::vector<DiagnosticEvent> &events);
    static std::string SafeFilename(const std::string &value);

    bool InDiagnosticWindow(int64_t timestamp_ns) const;

    std::string ServiceForInstance(const std::string &instance_id) const;

    std::unordered_map<std::string, std::string> instance_to_service_;
    std::unordered_map<std::string, std::unordered_map<int64_t, ActiveLifecycle>> active_;
    std::unordered_map<std::string, Series> instance_series_;
    std::unordered_map<std::string, Series> service_series_;
    std::unordered_map<std::string, size_t> service_delivery_cursors_;
    std::optional<int64_t> diagnostic_minute_start_ns_;
    std::vector<DiagnosticEvent> diagnostic_events_;
    bool service_exported_ = false;
    bool diagnostics_exported_ = false;
};

} // namespace kv_cache_manager
