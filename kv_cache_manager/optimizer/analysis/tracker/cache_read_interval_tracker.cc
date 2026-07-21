#include "kv_cache_manager/optimizer/analysis/tracker/cache_read_interval_tracker.h"

#include <algorithm>
#include <cmath>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <stdexcept>
#include <utility>

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
    return static_cast<double>(values[lower]) * (1.0 - fraction) +
           static_cast<double>(values[upper]) * fraction;
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

CacheReadIntervalTracker::CacheReadIntervalTracker() : StatsTracker("CacheReadIntervalTracker") {}

int64_t CacheReadIntervalTracker::MinuteStart(int64_t timestamp) {
    return timestamp >= 0 ? (timestamp / kMinuteNs) * kMinuteNs : 0;
}

void CacheReadIntervalTracker::ObserveTimestamp(Series *series, int64_t timestamp) {
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

CacheReadIntervalTracker::SummaryRow CacheReadIntervalTracker::Summarize(int64_t minute_start_ns, Bucket bucket) {
    SummaryRow row;
    row.minute_start_ns = minute_start_ns;
    row.interval_samples = bucket.intervals_ns.size();
    if (!bucket.intervals_ns.empty()) {
        row.average_ns = Average(bucket.intervals_ns);
        row.p50_ns = Quantile(bucket.intervals_ns, 0.50);
        row.p75_ns = Quantile(bucket.intervals_ns, 0.75);
        row.p95_ns = Quantile(bucket.intervals_ns, 0.95);
        row.p99_ns = Quantile(bucket.intervals_ns, 0.99);
    }
    return row;
}

void CacheReadIntervalTracker::AddSample(Series *series, int64_t timestamp, int64_t interval_ns) {
    if (series == nullptr) {
        return;
    }
    ObserveTimestamp(series, timestamp);
    const int64_t minute = MinuteStart(timestamp);
    if (!series->current_minute_ns.has_value()) {
        series->current_minute_ns = series->observation_start_minute_ns.value_or(minute);
    }
    if (minute < *series->current_minute_ns) {
        throw std::runtime_error("Cache read interval timestamps are not monotonic");
    }
    while (*series->current_minute_ns < minute) {
        series->rows.push_back(Summarize(*series->current_minute_ns, std::move(series->current_bucket)));
        series->current_bucket = Bucket{};
        *series->current_minute_ns += kMinuteNs;
    }
    series->current_bucket.intervals_ns.push_back(interval_ns);
    const int64_t upper_second = interval_ns <= 0 ? 0 : 1 + (interval_ns - 1) / kSecondNs;
    ++series->histogram_by_upper_second[upper_second];
}

void CacheReadIntervalTracker::FlushThrough(Series *series, int64_t final_timestamp) {
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

void CacheReadIntervalTracker::OnBlockBirth(const std::string &instance_id,
                                            BlockEntry *block,
                                            int64_t timestamp) {
    if (block == nullptr) {
        return;
    }
    last_read_hit_ns_[instance_id].erase(block->key);
    OnTimestamp(instance_id, timestamp);
}

void CacheReadIntervalTracker::OnBlockReadHit(const std::string &instance_id,
                                              BlockEntry *block,
                                              int64_t timestamp) {
    if (block == nullptr) {
        return;
    }
    auto &last_reads = last_read_hit_ns_[instance_id];
    auto it = last_reads.find(block->key);
    if (it != last_reads.end()) {
        if (timestamp < it->second) {
            throw std::runtime_error("Cache read interval event time precedes previous read hit");
        }
        AddSample(&instance_series_[instance_id], timestamp, timestamp - it->second);
        it->second = timestamp;
    } else {
        last_reads.emplace(block->key, timestamp);
        OnTimestamp(instance_id, timestamp);
    }
}

void CacheReadIntervalTracker::OnBlockRetentionEviction(const std::string &instance_id,
                                                        BlockEntry *block,
                                                        int64_t timestamp) {
    if (block == nullptr) {
        return;
    }
    auto instance_it = last_read_hit_ns_.find(instance_id);
    if (instance_it != last_read_hit_ns_.end()) {
        instance_it->second.erase(block->key);
    }
    OnTimestamp(instance_id, timestamp);
}

void CacheReadIntervalTracker::OnTimestamp(const std::string &instance_id, int64_t timestamp) {
    ObserveTimestamp(&instance_series_[instance_id], timestamp);
}

void CacheReadIntervalTracker::Finalize(const std::string &instance_id, int64_t final_timestamp) {
    FlushThrough(&instance_series_[instance_id], final_timestamp);
}

std::string CacheReadIntervalTracker::SafeFilename(const std::string &value) {
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

void CacheReadIntervalTracker::WriteCsv(const std::string &filename, const std::vector<SummaryRow> &rows) {
    std::ofstream out(filename);
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open cache read interval CSV: " + filename);
    }
    out << "MinuteStartNs,MinuteStart,IntervalSamples,ReadIntervalAverageSeconds,"
           "ReadIntervalP50Seconds,ReadIntervalP75Seconds,ReadIntervalP95Seconds,"
           "ReadIntervalP99Seconds\n";
    for (const auto &row : rows) {
        out << row.minute_start_ns << ',' << FormatMinute(row.minute_start_ns) << ',' << row.interval_samples << ',';
        WriteOptionalSeconds(out, row.average_ns);
        out << ',';
        WriteOptionalSeconds(out, row.p50_ns);
        out << ',';
        WriteOptionalSeconds(out, row.p75_ns);
        out << ',';
        WriteOptionalSeconds(out, row.p95_ns);
        out << ',';
        WriteOptionalSeconds(out, row.p99_ns);
        out << '\n';
    }
}

void CacheReadIntervalTracker::WriteHistogramCsv(
    const std::string &filename,
    const std::map<int64_t, uint64_t> &histogram) {
    std::ofstream out(filename);
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open cache read interval histogram CSV: " + filename);
    }
    out << "IntervalUpperSeconds,IntervalSamples,SamplesAtOrAboveBucket,FractionAtOrAboveBucket\n";
    uint64_t remaining = 0;
    for (const auto &[_, samples] : histogram) {
        remaining += samples;
    }
    const uint64_t total = remaining;
    for (const auto &[upper_second, samples] : histogram) {
        out << upper_second << ',' << samples << ',' << remaining << ',';
        if (total > 0) {
            out << std::fixed << std::setprecision(12)
                << static_cast<long double>(remaining) / static_cast<long double>(total);
        }
        out << '\n';
        remaining -= samples;
    }
}

void CacheReadIntervalTracker::Export(const std::string &instance_id, const OptimizerConfig &config) {
    std::filesystem::create_directories(config.output_result_path());
    auto it = instance_series_.find(instance_id);
    if (it != instance_series_.end()) {
        const std::string prefix = config.output_result_path() + "/" + SafeFilename(instance_id);
        WriteCsv(prefix + "_cache_read_interval_by_minute.csv",
                 it->second.rows);
        WriteHistogramCsv(prefix + "_cache_read_interval_histogram.csv",
                          it->second.histogram_by_upper_second);
    }
}

void CacheReadIntervalTracker::Reset(const std::string &instance_id) {
    last_read_hit_ns_.erase(instance_id);
    instance_series_.erase(instance_id);
}

const std::vector<CacheReadIntervalTracker::SummaryRow> &
CacheReadIntervalTracker::InstanceRows(const std::string &instance_id) const {
    static const std::vector<SummaryRow> empty;
    auto it = instance_series_.find(instance_id);
    return it == instance_series_.end() ? empty : it->second.rows;
}

const std::map<int64_t, uint64_t> &
CacheReadIntervalTracker::InstanceHistogram(const std::string &instance_id) const {
    static const std::map<int64_t, uint64_t> empty;
    auto it = instance_series_.find(instance_id);
    return it == instance_series_.end() ? empty : it->second.histogram_by_upper_second;
}

} // namespace kv_cache_manager
