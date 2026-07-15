#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
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
        std::optional<double> lifetime_p50_ns;
        std::optional<double> lifetime_p75_ns;
        std::optional<double> lifetime_p99_ns;
        std::optional<double> idle_average_ns;
        std::optional<double> idle_p50_ns;
        std::optional<double> idle_p75_ns;
        std::optional<double> idle_p99_ns;
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

private:
    struct ActiveLifecycle {
        int64_t birth_time_ns = 0;
        std::optional<int64_t> last_read_hit_time_ns;
    };

    struct Bucket {
        size_t evicted_blocks = 0;
        size_t reused_evicted_blocks = 0;
        std::vector<int64_t> lifetimes_ns;
        std::vector<int64_t> idle_after_reuse_ns;
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
                          const std::optional<int64_t> &idle_after_reuse_ns);
    static void ObserveTimestamp(Series *series, int64_t timestamp);
    static void FlushThrough(Series *series, int64_t final_timestamp);
    static void WriteCsv(const std::string &filename, const std::vector<SummaryRow> &rows);
    static std::string SafeFilename(const std::string &value);

    std::string ServiceForInstance(const std::string &instance_id) const;

    std::unordered_map<std::string, std::string> instance_to_service_;
    std::unordered_map<std::string, std::unordered_map<int64_t, ActiveLifecycle>> active_;
    std::unordered_map<std::string, Series> instance_series_;
    std::unordered_map<std::string, Series> service_series_;
    bool service_exported_ = false;
};

} // namespace kv_cache_manager
