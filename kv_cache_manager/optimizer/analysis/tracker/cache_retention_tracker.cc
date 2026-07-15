#include "kv_cache_manager/optimizer/analysis/tracker/cache_retention_tracker.h"

#include <algorithm>
#include <cmath>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <numeric>
#include <stdexcept>

#include "kv_cache_manager/common/logger.h"
#include "kv_cache_manager/optimizer/config/optimizer_config.h"
#include "kv_cache_manager/optimizer/config/types.h"

namespace kv_cache_manager {
namespace {

double Quantile(std::vector<int64_t> values, double quantile) {
    if (values.empty()) {
        throw std::invalid_argument("Quantile requires non-empty values");
    }
    std::sort(values.begin(), values.end());
    const double position = quantile * static_cast<double>(values.size() - 1);
    const size_t lower = static_cast<size_t>(std::floor(position));
    const size_t upper = static_cast<size_t>(std::ceil(position));
    const double fraction = position - static_cast<double>(lower);
    return static_cast<double>(values[lower]) * (1.0 - fraction) + static_cast<double>(values[upper]) * fraction;
}

double Average(const std::vector<int64_t> &values) {
    long double sum = 0.0;
    for (const int64_t value : values) {
        sum += static_cast<long double>(value);
    }
    return static_cast<double>(sum / static_cast<long double>(values.size()));
}

std::string FormatMinute(int64_t timestamp_ns) {
    constexpr int64_t kShanghaiOffsetSeconds = 8 * 60 * 60;
    const std::time_t shifted = static_cast<std::time_t>(timestamp_ns / 1000000000 + kShanghaiOffsetSeconds);
    std::tm tm{};
    gmtime_r(&shifted, &tm);
    char buffer[40];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S+08:00", &tm);
    return buffer;
}

void WriteOptionalSeconds(std::ostream &out, const std::optional<double> &value_ns) {
    if (value_ns.has_value()) {
        out << std::fixed << std::setprecision(9) << (*value_ns / 1000000000.0);
    }
}

} // namespace

CacheRetentionTracker::CacheRetentionTracker(std::unordered_map<std::string, std::string> instance_to_service)
    : StatsTracker("CacheRetentionTracker"), instance_to_service_(std::move(instance_to_service)) {}

int64_t CacheRetentionTracker::MinuteStart(int64_t timestamp) {
    return timestamp >= 0 ? (timestamp / kMinuteNs) * kMinuteNs : 0;
}

void CacheRetentionTracker::ObserveTimestamp(Series *series, int64_t timestamp) {
    if (series == nullptr || timestamp < 0) {
        return;
    }
    const int64_t minute = MinuteStart(timestamp);
    if (!series->observation_start_minute_ns.has_value() || minute < *series->observation_start_minute_ns) {
        series->observation_start_minute_ns = minute;
    }
    if (!series->observation_end_minute_ns.has_value() || minute > *series->observation_end_minute_ns) {
        series->observation_end_minute_ns = minute;
    }
}

CacheRetentionTracker::SummaryRow CacheRetentionTracker::Summarize(int64_t minute_start_ns, Bucket bucket) {
    SummaryRow row;
    row.minute_start_ns = minute_start_ns;
    row.evicted_blocks = bucket.evicted_blocks;
    row.reused_evicted_blocks = bucket.reused_evicted_blocks;
    row.never_reused_evicted_blocks = bucket.evicted_blocks - bucket.reused_evicted_blocks;
    if (!bucket.lifetimes_ns.empty()) {
        row.lifetime_average_ns = Average(bucket.lifetimes_ns);
        row.lifetime_p50_ns = Quantile(bucket.lifetimes_ns, 0.50);
        row.lifetime_p75_ns = Quantile(bucket.lifetimes_ns, 0.75);
        row.lifetime_p99_ns = Quantile(bucket.lifetimes_ns, 0.99);
    }
    if (!bucket.idle_after_reuse_ns.empty()) {
        row.idle_average_ns = Average(bucket.idle_after_reuse_ns);
        row.idle_p50_ns = Quantile(bucket.idle_after_reuse_ns, 0.50);
        row.idle_p75_ns = Quantile(bucket.idle_after_reuse_ns, 0.75);
        row.idle_p99_ns = Quantile(bucket.idle_after_reuse_ns, 0.99);
    }
    return row;
}

void CacheRetentionTracker::AddSample(Series *series,
                                      int64_t eviction_timestamp,
                                      int64_t lifetime_ns,
                                      const std::optional<int64_t> &idle_after_reuse_ns) {
    if (series == nullptr) {
        return;
    }
    ObserveTimestamp(series, eviction_timestamp);
    const int64_t minute = MinuteStart(eviction_timestamp);
    if (!series->current_minute_ns.has_value()) {
        series->current_minute_ns = series->observation_start_minute_ns.value_or(minute);
    }
    if (minute < *series->current_minute_ns) {
        throw std::runtime_error("Cache retention eviction timestamps are not monotonic");
    }
    while (*series->current_minute_ns < minute) {
        series->rows.push_back(Summarize(*series->current_minute_ns, std::move(series->current_bucket)));
        series->current_bucket = Bucket{};
        *series->current_minute_ns += kMinuteNs;
    }
    series->current_bucket.evicted_blocks += 1;
    series->current_bucket.lifetimes_ns.push_back(lifetime_ns);
    if (idle_after_reuse_ns.has_value()) {
        series->current_bucket.reused_evicted_blocks += 1;
        series->current_bucket.idle_after_reuse_ns.push_back(*idle_after_reuse_ns);
    }
}

void CacheRetentionTracker::FlushThrough(Series *series, int64_t final_timestamp) {
    if (series == nullptr || series->finalized) {
        return;
    }
    ObserveTimestamp(series, final_timestamp);
    if (!series->observation_start_minute_ns.has_value() || !series->observation_end_minute_ns.has_value()) {
        series->finalized = true;
        return;
    }
    if (!series->current_minute_ns.has_value()) {
        series->current_minute_ns = *series->observation_start_minute_ns;
    }
    while (*series->current_minute_ns <= *series->observation_end_minute_ns) {
        series->rows.push_back(Summarize(*series->current_minute_ns, std::move(series->current_bucket)));
        series->current_bucket = Bucket{};
        *series->current_minute_ns += kMinuteNs;
    }
    series->finalized = true;
}

std::string CacheRetentionTracker::ServiceForInstance(const std::string &instance_id) const {
    auto it = instance_to_service_.find(instance_id);
    return it == instance_to_service_.end() || it->second.empty() ? instance_id : it->second;
}

void CacheRetentionTracker::OnTimestamp(const std::string &instance_id, int64_t timestamp) {
    ObserveTimestamp(&instance_series_[instance_id], timestamp);
    ObserveTimestamp(&service_series_[ServiceForInstance(instance_id)], timestamp);
}

void CacheRetentionTracker::OnBlockBirth(const std::string &instance_id, BlockEntry *block, int64_t timestamp) {
    if (block == nullptr) {
        return;
    }
    auto &lifecycles = active_[instance_id];
    auto [it, inserted] = lifecycles.emplace(block->key, ActiveLifecycle{timestamp, std::nullopt});
    if (!inserted) {
        KVCM_LOG_WARN("Resetting duplicate active retention lifecycle: instance=%s key=%lld",
                      instance_id.c_str(),
                      static_cast<long long>(block->key));
        it->second = ActiveLifecycle{timestamp, std::nullopt};
    }
    OnTimestamp(instance_id, timestamp);
}

void CacheRetentionTracker::OnBlockReadHit(const std::string &instance_id, BlockEntry *block, int64_t timestamp) {
    if (block == nullptr) {
        return;
    }
    auto inst_it = active_.find(instance_id);
    if (inst_it == active_.end()) {
        return;
    }
    auto lifecycle_it = inst_it->second.find(block->key);
    if (lifecycle_it == inst_it->second.end()) {
        return;
    }
    lifecycle_it->second.last_read_hit_time_ns = timestamp;
    OnTimestamp(instance_id, timestamp);
}

void CacheRetentionTracker::OnBlockRetentionEviction(const std::string &instance_id,
                                                      BlockEntry *block,
                                                      int64_t timestamp) {
    if (block == nullptr) {
        return;
    }
    auto inst_it = active_.find(instance_id);
    if (inst_it == active_.end()) {
        return;
    }
    auto lifecycle_it = inst_it->second.find(block->key);
    if (lifecycle_it == inst_it->second.end()) {
        return;
    }
    const ActiveLifecycle lifecycle = lifecycle_it->second;
    if (timestamp < lifecycle.birth_time_ns ||
        (lifecycle.last_read_hit_time_ns.has_value() && timestamp < *lifecycle.last_read_hit_time_ns)) {
        throw std::runtime_error("Cache retention event time precedes lifecycle time");
    }
    const int64_t lifetime_ns = timestamp - lifecycle.birth_time_ns;
    const std::optional<int64_t> idle_ns = lifecycle.last_read_hit_time_ns.has_value()
                                                ? std::optional<int64_t>(timestamp - *lifecycle.last_read_hit_time_ns)
                                                : std::nullopt;
    AddSample(&instance_series_[instance_id], timestamp, lifetime_ns, idle_ns);
    AddSample(&service_series_[ServiceForInstance(instance_id)], timestamp, lifetime_ns, idle_ns);
    inst_it->second.erase(lifecycle_it);
}

void CacheRetentionTracker::Finalize(const std::string &instance_id, int64_t final_timestamp) {
    FlushThrough(&instance_series_[instance_id], final_timestamp);
    // Alive lifecycles are right-censored and intentionally excluded from duration samples.
}

std::string CacheRetentionTracker::SafeFilename(const std::string &value) {
    std::string result = value;
    for (char &ch : result) {
        const bool safe = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                          (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.';
        if (!safe) {
            ch = '_';
        }
    }
    return result.empty() ? "unnamed" : result;
}

void CacheRetentionTracker::WriteCsv(const std::string &filename, const std::vector<SummaryRow> &rows) {
    std::ofstream out(filename);
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open cache retention CSV: " + filename);
    }
    out << "MinuteStartNs,MinuteStart,EvictedBlocks,ReusedEvictedBlocks,NeverReusedEvictedBlocks,"
           "LifetimeAverageSeconds,LifetimeP50Seconds,LifetimeP75Seconds,LifetimeP99Seconds,"
           "IdleAfterReuseAverageSeconds,IdleAfterReuseP50Seconds,IdleAfterReuseP75Seconds,"
           "IdleAfterReuseP99Seconds\n";
    for (const auto &row : rows) {
        out << row.minute_start_ns << ',' << FormatMinute(row.minute_start_ns) << ',' << row.evicted_blocks << ','
            << row.reused_evicted_blocks << ',' << row.never_reused_evicted_blocks << ',';
        WriteOptionalSeconds(out, row.lifetime_average_ns);
        out << ',';
        WriteOptionalSeconds(out, row.lifetime_p50_ns);
        out << ',';
        WriteOptionalSeconds(out, row.lifetime_p75_ns);
        out << ',';
        WriteOptionalSeconds(out, row.lifetime_p99_ns);
        out << ',';
        WriteOptionalSeconds(out, row.idle_average_ns);
        out << ',';
        WriteOptionalSeconds(out, row.idle_p50_ns);
        out << ',';
        WriteOptionalSeconds(out, row.idle_p75_ns);
        out << ',';
        WriteOptionalSeconds(out, row.idle_p99_ns);
        out << '\n';
    }
}

void CacheRetentionTracker::Export(const std::string &instance_id, const OptimizerConfig &config) {
    std::filesystem::create_directories(config.output_result_path());
    auto inst_it = instance_series_.find(instance_id);
    if (inst_it != instance_series_.end()) {
        WriteCsv(config.output_result_path() + "/" + SafeFilename(instance_id) + "_cache_retention_by_minute.csv",
                 inst_it->second.rows);
    }
    if (!service_exported_) {
        for (auto &[service_name, series] : service_series_) {
            const int64_t final_timestamp = series.observation_end_minute_ns.value_or(0) + kMinuteNs - 1;
            FlushThrough(&series, final_timestamp);
            WriteCsv(config.output_result_path() + "/service_" + SafeFilename(service_name) +
                         "_cache_retention_by_minute.csv",
                     series.rows);
        }
        service_exported_ = true;
    }
}

void CacheRetentionTracker::Reset(const std::string &instance_id) {
    active_.erase(instance_id);
    instance_series_.erase(instance_id);
}

const std::vector<CacheRetentionTracker::SummaryRow> &
CacheRetentionTracker::InstanceRows(const std::string &instance_id) const {
    static const std::vector<SummaryRow> empty;
    auto it = instance_series_.find(instance_id);
    return it == instance_series_.end() ? empty : it->second.rows;
}

const std::vector<CacheRetentionTracker::SummaryRow> &
CacheRetentionTracker::ServiceRows(const std::string &service_name) const {
    static const std::vector<SummaryRow> empty;
    auto it = service_series_.find(service_name);
    return it == service_series_.end() ? empty : it->second.rows;
}

} // namespace kv_cache_manager
