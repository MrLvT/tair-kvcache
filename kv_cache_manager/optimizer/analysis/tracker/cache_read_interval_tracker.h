#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "kv_cache_manager/optimizer/analysis/stats_tracker.h"

namespace kv_cache_manager {

class CacheReadIntervalTracker : public StatsTracker {
public:
    static constexpr int64_t kMinuteNs = 60LL * 1000 * 1000 * 1000;
    static constexpr int64_t kSecondNs = 1000LL * 1000 * 1000;

    struct SummaryRow {
        int64_t minute_start_ns = 0;
        size_t interval_samples = 0;
        std::optional<double> average_ns;
        std::optional<double> p50_ns;
        std::optional<double> p75_ns;
        std::optional<double> p95_ns;
        std::optional<double> p99_ns;
    };

    CacheReadIntervalTracker();

    void OnBlockBirth(const std::string &instance_id, BlockEntry *block, int64_t timestamp) override;
    void OnBlockReadHit(const std::string &instance_id, BlockEntry *block, int64_t timestamp) override;
    void OnBlockRetentionEviction(const std::string &instance_id, BlockEntry *block, int64_t timestamp) override;
    void OnTimestamp(const std::string &instance_id, int64_t timestamp) override;
    void Finalize(const std::string &instance_id, int64_t final_timestamp) override;
    void Export(const std::string &instance_id, const OptimizerConfig &config) override;
    void Reset(const std::string &instance_id) override;

    const std::vector<SummaryRow> &InstanceRows(const std::string &instance_id) const;
    const std::map<int64_t, uint64_t> &InstanceHistogram(const std::string &instance_id) const;

private:
    struct Bucket {
        std::vector<int64_t> intervals_ns;
    };

    struct Series {
        std::optional<int64_t> observation_start_minute_ns;
        std::optional<int64_t> observation_end_minute_ns;
        std::optional<int64_t> current_minute_ns;
        Bucket current_bucket;
        std::vector<SummaryRow> rows;
        std::map<int64_t, uint64_t> histogram_by_upper_second;
        bool finalized = false;
    };

    static int64_t MinuteStart(int64_t timestamp);
    static SummaryRow Summarize(int64_t minute_start_ns, Bucket bucket);
    static void ObserveTimestamp(Series *series, int64_t timestamp);
    static void AddSample(Series *series, int64_t timestamp, int64_t interval_ns);
    static void FlushThrough(Series *series, int64_t final_timestamp);
    static void WriteCsv(const std::string &filename, const std::vector<SummaryRow> &rows);
    static void WriteHistogramCsv(const std::string &filename,
                                  const std::map<int64_t, uint64_t> &histogram);
    static std::string SafeFilename(const std::string &value);

    std::unordered_map<std::string, std::unordered_map<int64_t, int64_t>> last_read_hit_ns_;
    std::unordered_map<std::string, Series> instance_series_;
};

} // namespace kv_cache_manager
