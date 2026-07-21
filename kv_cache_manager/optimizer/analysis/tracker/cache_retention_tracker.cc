#include "kv_cache_manager/optimizer/analysis/tracker/cache_retention_tracker.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>
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

double QuantileDouble(std::vector<double> values, double quantile) {
    if (values.empty()) {
        throw std::invalid_argument("QuantileDouble requires non-empty values");
    }
    std::sort(values.begin(), values.end());
    const double position = quantile * static_cast<double>(values.size() - 1);
    const size_t lower = static_cast<size_t>(std::floor(position));
    const size_t upper = static_cast<size_t>(std::ceil(position));
    const double fraction = position - static_cast<double>(lower);
    return values[lower] * (1.0 - fraction) + values[upper] * fraction;
}

double AverageDouble(const std::vector<double> &values) {
    const long double sum = std::accumulate(values.begin(), values.end(), static_cast<long double>(0.0));
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
    : StatsTracker("CacheRetentionTracker"), instance_to_service_(std::move(instance_to_service)) {
    const char *diagnostic_minute = std::getenv("KVCM_RETENTION_DIAGNOSTIC_MINUTE_START_NS");
    if (diagnostic_minute != nullptr && diagnostic_minute[0] != '\0') {
        try {
            diagnostic_minute_start_ns_ = std::stoll(diagnostic_minute);
            KVCM_LOG_INFO("Cache retention detail diagnostics enabled for minute_start_ns=%lld",
                          static_cast<long long>(*diagnostic_minute_start_ns_));
        } catch (const std::exception &) {
            throw std::invalid_argument("KVCM_RETENTION_DIAGNOSTIC_MINUTE_START_NS must be an int64 timestamp");
        }
    }
}

int64_t CacheRetentionTracker::MinuteStart(int64_t timestamp) {
    return timestamp >= 0 ? (timestamp / kMinuteNs) * kMinuteNs : 0;
}

bool CacheRetentionTracker::InDiagnosticWindow(int64_t timestamp_ns) const {
    return diagnostic_minute_start_ns_.has_value() && timestamp_ns >= *diagnostic_minute_start_ns_ &&
           timestamp_ns < *diagnostic_minute_start_ns_ + kMinuteNs;
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
        row.lifetime_p10_ns = Quantile(bucket.lifetimes_ns, 0.10);
        row.lifetime_p50_ns = Quantile(bucket.lifetimes_ns, 0.50);
        row.lifetime_p75_ns = Quantile(bucket.lifetimes_ns, 0.75);
        row.lifetime_p95_ns = Quantile(bucket.lifetimes_ns, 0.95);
        row.lifetime_p99_ns = Quantile(bucket.lifetimes_ns, 0.99);
    }
    if (!bucket.idle_after_reuse_ns.empty()) {
        row.idle_average_ns = Average(bucket.idle_after_reuse_ns);
        row.idle_p10_ns = Quantile(bucket.idle_after_reuse_ns, 0.10);
        row.idle_p50_ns = Quantile(bucket.idle_after_reuse_ns, 0.50);
        row.idle_p75_ns = Quantile(bucket.idle_after_reuse_ns, 0.75);
        row.idle_p95_ns = Quantile(bucket.idle_after_reuse_ns, 0.95);
        row.idle_p99_ns = Quantile(bucket.idle_after_reuse_ns, 0.99);
    }
    if (bucket.lru_time_span_ns.has_value()) {
        row.lru_time_span_ns = static_cast<double>(*bucket.lru_time_span_ns);
    }
    if (!bucket.all_block_last_touch_ages_ns.empty()) {
        row.all_block_last_touch_age_average_ns = Average(bucket.all_block_last_touch_ages_ns);
        row.all_block_last_touch_age_p10_ns = Quantile(bucket.all_block_last_touch_ages_ns, 0.10);
        row.all_block_last_touch_age_p50_ns = Quantile(bucket.all_block_last_touch_ages_ns, 0.50);
        row.all_block_last_touch_age_p95_ns = Quantile(bucket.all_block_last_touch_ages_ns, 0.95);
    }
    row.distinct_eviction_timestamps = bucket.eviction_timestamps_ns.size();
    row.distinct_last_read_timestamps = bucket.last_read_timestamp_counts.size();
    for (const auto &[_, count] : bucket.last_read_timestamp_counts) {
        row.largest_last_read_cohort_blocks = std::max(row.largest_last_read_cohort_blocks, count);
    }
    if (row.reused_evicted_blocks > 0) {
        row.largest_last_read_cohort_fraction =
            static_cast<double>(row.largest_last_read_cohort_blocks) / static_cast<double>(row.reused_evicted_blocks);
    }
    std::vector<double> batch_idle_spreads_ns;
    batch_idle_spreads_ns.reserve(bucket.eviction_batch_idle_ns.size());
    for (const auto &[_, idle_samples] : bucket.eviction_batch_idle_ns) {
        if (idle_samples.empty()) {
            continue;
        }
        batch_idle_spreads_ns.push_back(Quantile(idle_samples, 0.95) - Quantile(idle_samples, 0.10));
    }
    row.reuse_eviction_batches = batch_idle_spreads_ns.size();
    if (!batch_idle_spreads_ns.empty()) {
        row.batch_idle_spread_average_ns = AverageDouble(batch_idle_spreads_ns);
        row.batch_idle_spread_p50_ns = QuantileDouble(batch_idle_spreads_ns, 0.50);
        row.batch_idle_spread_p95_ns = QuantileDouble(batch_idle_spreads_ns, 0.95);
    }
    return row;
}

void CacheRetentionTracker::AddSample(Series *series,
                                      int64_t eviction_timestamp,
                                      int64_t lifetime_ns,
                                      int64_t all_block_last_touch_age_ns,
                                      const std::optional<int64_t> &idle_after_reuse_ns,
                                      const std::optional<int64_t> &last_read_timestamp_ns) {
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
    series->current_bucket.all_block_last_touch_ages_ns.push_back(all_block_last_touch_age_ns);
    series->current_bucket.eviction_timestamps_ns.insert(eviction_timestamp);
    if (idle_after_reuse_ns.has_value()) {
        series->current_bucket.reused_evicted_blocks += 1;
        series->current_bucket.idle_after_reuse_ns.push_back(*idle_after_reuse_ns);
        series->current_bucket.eviction_batch_idle_ns[eviction_timestamp].push_back(*idle_after_reuse_ns);
        if (last_read_timestamp_ns.has_value()) {
            series->current_bucket.last_read_timestamp_counts[*last_read_timestamp_ns] += 1;
        }
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

void CacheRetentionTracker::FlushClosedThrough(Series *series, int64_t timestamp) {
    if (series == nullptr || series->finalized) {
        return;
    }
    if (!series->observation_start_minute_ns.has_value()) {
        return;
    }
    if (!series->current_minute_ns.has_value()) {
        series->current_minute_ns = *series->observation_start_minute_ns;
    }
    while (*series->current_minute_ns + kMinuteNs <= timestamp) {
        series->rows.push_back(Summarize(*series->current_minute_ns, std::move(series->current_bucket)));
        series->current_bucket = Bucket{};
        *series->current_minute_ns += kMinuteNs;
    }
}

std::vector<CacheRetentionTracker::SummaryRow>
CacheRetentionTracker::TakeClosedServiceRowsThrough(const std::string &service_name, int64_t timestamp) {
    auto &series = service_series_[service_name];
    FlushClosedThrough(&series, timestamp);
    size_t &cursor = service_delivery_cursors_[service_name];
    if (cursor > series.rows.size()) {
        cursor = series.rows.size();
    }
    std::vector<SummaryRow> result(series.rows.begin() + cursor, series.rows.end());
    cursor = series.rows.size();
    return result;
}

void CacheRetentionTracker::OnLruTimeSpanSnapshot(const std::string &instance_id,
                                                  int64_t minute_start_ns,
                                                  const std::optional<int64_t> &span_ns) {
    auto record = [minute_start_ns, &span_ns](Series *series) {
        if (series == nullptr || series->finalized) {
            return;
        }
        ObserveTimestamp(series, minute_start_ns);
        if (!series->current_minute_ns.has_value()) {
            series->current_minute_ns = series->observation_start_minute_ns.value_or(minute_start_ns);
        }
        while (*series->current_minute_ns < minute_start_ns) {
            series->rows.push_back(Summarize(*series->current_minute_ns, std::move(series->current_bucket)));
            series->current_bucket = Bucket{};
            *series->current_minute_ns += kMinuteNs;
        }
        if (*series->current_minute_ns != minute_start_ns) {
            throw std::runtime_error("LRU time span snapshot does not match current retention minute");
        }
        series->current_bucket.lru_time_span_ns = span_ns;
    };
    record(&instance_series_[instance_id]);
    record(&service_series_[ServiceForInstance(instance_id)]);
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
    if (InDiagnosticWindow(timestamp)) {
        diagnostic_events_.push_back(
            DiagnosticEvent{"BIRTH", instance_id, block->key, timestamp, timestamp, std::nullopt, std::nullopt});
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
    const int64_t last_touch_age_ns = idle_ns.value_or(lifetime_ns);
    if (InDiagnosticWindow(timestamp)) {
        diagnostic_events_.push_back(DiagnosticEvent{"EVICTION",
                                                     instance_id,
                                                     block->key,
                                                     timestamp,
                                                     lifecycle.birth_time_ns,
                                                     lifecycle.last_read_hit_time_ns,
                                                     timestamp});
    }
    AddSample(&instance_series_[instance_id],
              timestamp,
              lifetime_ns,
              last_touch_age_ns,
              idle_ns,
              lifecycle.last_read_hit_time_ns);
    AddSample(&service_series_[ServiceForInstance(instance_id)],
              timestamp,
              lifetime_ns,
              last_touch_age_ns,
              idle_ns,
              lifecycle.last_read_hit_time_ns);
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
           "LifetimeAverageSeconds,LifetimeP10Seconds,LifetimeP50Seconds,LifetimeP75Seconds,LifetimeP95Seconds,"
           "LifetimeP99Seconds,IdleAfterReuseAverageSeconds,IdleAfterReuseP10Seconds,IdleAfterReuseP50Seconds,"
           "IdleAfterReuseP75Seconds,IdleAfterReuseP95Seconds,"
           "IdleAfterReuseP99Seconds,LruTimeSpanSeconds,AllBlockLastTouchAgeAverageSeconds,"
           "AllBlockLastTouchAgeP10Seconds,"
           "AllBlockLastTouchAgeP50Seconds,AllBlockLastTouchAgeP95Seconds,DistinctEvictionTimestamps,"
           "DistinctLastReadTimestamps,LargestLastReadCohortBlocks,LargestLastReadCohortFraction,"
           "ReuseEvictionBatches,BatchIdleSpreadAverageSeconds,BatchIdleSpreadP50Seconds,"
           "BatchIdleSpreadP95Seconds\n";
    for (const auto &row : rows) {
        out << row.minute_start_ns << ',' << FormatMinute(row.minute_start_ns) << ',' << row.evicted_blocks << ','
            << row.reused_evicted_blocks << ',' << row.never_reused_evicted_blocks << ',';
        WriteOptionalSeconds(out, row.lifetime_average_ns);
        out << ',';
        WriteOptionalSeconds(out, row.lifetime_p10_ns);
        out << ',';
        WriteOptionalSeconds(out, row.lifetime_p50_ns);
        out << ',';
        WriteOptionalSeconds(out, row.lifetime_p75_ns);
        out << ',';
        WriteOptionalSeconds(out, row.lifetime_p95_ns);
        out << ',';
        WriteOptionalSeconds(out, row.lifetime_p99_ns);
        out << ',';
        WriteOptionalSeconds(out, row.idle_average_ns);
        out << ',';
        WriteOptionalSeconds(out, row.idle_p10_ns);
        out << ',';
        WriteOptionalSeconds(out, row.idle_p50_ns);
        out << ',';
        WriteOptionalSeconds(out, row.idle_p75_ns);
        out << ',';
        WriteOptionalSeconds(out, row.idle_p95_ns);
        out << ',';
        WriteOptionalSeconds(out, row.idle_p99_ns);
        out << ',';
        WriteOptionalSeconds(out, row.lru_time_span_ns);
        out << ',';
        WriteOptionalSeconds(out, row.all_block_last_touch_age_average_ns);
        out << ',';
        WriteOptionalSeconds(out, row.all_block_last_touch_age_p10_ns);
        out << ',';
        WriteOptionalSeconds(out, row.all_block_last_touch_age_p50_ns);
        out << ',';
        WriteOptionalSeconds(out, row.all_block_last_touch_age_p95_ns);
        out << ',' << row.distinct_eviction_timestamps << ',' << row.distinct_last_read_timestamps << ','
            << row.largest_last_read_cohort_blocks << ',';
        if (row.largest_last_read_cohort_fraction.has_value()) {
            out << std::fixed << std::setprecision(9) << *row.largest_last_read_cohort_fraction;
        }
        out << ',' << row.reuse_eviction_batches << ',';
        WriteOptionalSeconds(out, row.batch_idle_spread_average_ns);
        out << ',';
        WriteOptionalSeconds(out, row.batch_idle_spread_p50_ns);
        out << ',';
        WriteOptionalSeconds(out, row.batch_idle_spread_p95_ns);
        out << '\n';
    }
}

void CacheRetentionTracker::WriteDiagnosticCsv(const std::string &filename,
                                               const std::vector<DiagnosticEvent> &events) {
    std::ofstream out(filename);
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open cache retention diagnostic CSV: " + filename);
    }
    out << "EventType,InstanceId,BlockKey,EventTimestampNs,BirthTimeNs,LastReadHitTimeNs,LastTouchTimeNs,"
           "EvictionTimeNs,LifetimeSeconds,IdleAfterReuseSeconds,AgeSinceLastTouchSeconds\n";
    for (const auto &event : events) {
        out << event.event_type << ',' << event.instance_id << ',' << event.block_key << ','
            << event.event_timestamp_ns << ',' << event.birth_time_ns << ',';
        if (event.last_read_hit_time_ns.has_value()) {
            out << *event.last_read_hit_time_ns;
        }
        const int64_t last_touch_time_ns = event.last_read_hit_time_ns.value_or(event.birth_time_ns);
        out << ',' << last_touch_time_ns << ',';
        if (event.eviction_time_ns.has_value()) {
            out << *event.eviction_time_ns << ',';
            WriteOptionalSeconds(out,
                                 std::optional<double>(static_cast<double>(*event.eviction_time_ns -
                                                                          event.birth_time_ns)));
            out << ',';
            const std::optional<double> idle_ns = event.last_read_hit_time_ns.has_value()
                                                       ? std::optional<double>(static_cast<double>(
                                                             *event.eviction_time_ns -
                                                             *event.last_read_hit_time_ns))
                                                       : std::nullopt;
            WriteOptionalSeconds(out, idle_ns);
            out << ',';
            WriteOptionalSeconds(out,
                                 std::optional<double>(static_cast<double>(*event.eviction_time_ns -
                                                                          last_touch_time_ns)));
        } else {
            out << ",,,,";
        }
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
    if (!diagnostics_exported_ && diagnostic_minute_start_ns_.has_value()) {
        WriteDiagnosticCsv(config.output_result_path() + "/cache_retention_diagnostic_" +
                               std::to_string(*diagnostic_minute_start_ns_) + ".csv",
                           diagnostic_events_);
        diagnostics_exported_ = true;
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
