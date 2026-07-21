#include "kv_cache_manager/optimizer/manager/cache_capacity_autoscaler.h"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <stdexcept>
#include <ctime>
#include <utility>

namespace kv_cache_manager {
namespace {

std::string FormatTimestamp(int64_t timestamp_ns) {
    constexpr int64_t kShanghaiOffsetSeconds = 8 * 60 * 60;
    const std::time_t shifted =
        static_cast<std::time_t>(timestamp_ns / CacheCapacityAutoscaler::kSecondNs + kShanghaiOffsetSeconds);
    std::tm tm{};
    gmtime_r(&shifted, &tm);
    char buffer[40];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S+08:00", &tm);
    return buffer;
}

} // namespace

CacheCapacityAutoscaler::CacheCapacityAutoscaler(OptCacheAutoscalingConfig config,
                                                 std::string target_group,
                                                 int64_t baseline_capacity_bytes)
    : config_(std::move(config))
    , target_group_(std::move(target_group))
    , baseline_capacity_bytes_(baseline_capacity_bytes)
    , current_capacity_bytes_(baseline_capacity_bytes) {
    if (baseline_capacity_bytes_ <= 0) {
        throw std::invalid_argument("cache autoscaling requires a positive baseline capacity");
    }
}

void CacheCapacityAutoscaler::Schedule(const CacheRetentionTracker::SummaryRow &row,
                                       double metric_seconds,
                                       int64_t trigger_time_ns,
                                       int64_t effective_time_ns,
                                       int64_t delta_capacity_bytes,
                                       const std::string &action,
                                       const std::string &reason) {
    Event event;
    event.event_id = events_.size() + 1;
    event.metric_minute_start_ns = row.minute_start_ns;
    event.metric_value_seconds = metric_seconds;
    event.evicted_blocks = row.evicted_blocks;
    event.action = action;
    event.trigger_time_ns = trigger_time_ns;
    event.scheduled_effective_time_ns = effective_time_ns;
    event.delta_capacity_bytes = delta_capacity_bytes;
    event.capacity_before_bytes = current_capacity_bytes_;
    event.reason = reason;
    events_.push_back(std::move(event));
    pending_event_index_ = events_.size() - 1;
}

void CacheCapacityAutoscaler::OnMetric(const CacheRetentionTracker::SummaryRow &row,
                                       int64_t decision_time_ns) {
    if (!row.idle_p10_ns.has_value() || row.evicted_blocks == 0 || pending_event_index_.has_value()) {
        return;
    }
    const double seconds = *row.idle_p10_ns / static_cast<double>(kSecondNs);
    if (seconds < config_.scale_out_threshold_seconds()) {
        if (previous_zone_ != Zone::LOW) {
            if (config_.scale_step_tib() > std::numeric_limits<int64_t>::max() / kTiB) {
                throw std::overflow_error("cache autoscaling scale_step_tib overflows int64");
            }
            const int64_t delta = config_.scale_step_tib() * kTiB;
            const int64_t delay = config_.scale_out_delay_seconds() * kSecondNs;
            Schedule(row,
                     seconds,
                     decision_time_ns,
                     decision_time_ns + delay,
                     delta,
                     "SCALE_OUT",
                     "idle_p10_below_scale_out_threshold");
        }
        previous_zone_ = Zone::LOW;
    } else if (seconds > config_.scale_in_threshold_seconds()) {
        if (previous_zone_ != Zone::HIGH && extra_units_ > 0) {
            Schedule(row,
                     seconds,
                     decision_time_ns,
                     decision_time_ns,
                     -config_.scale_step_tib() * kTiB,
                     "SCALE_IN",
                     "idle_p10_above_scale_in_threshold");
        }
        previous_zone_ = Zone::HIGH;
    } else {
        previous_zone_ = Zone::DEAD_BAND;
    }
}

void CacheCapacityAutoscaler::ApplyDueThrough(int64_t timestamp_ns, const ApplyCallback &apply) {
    if (!pending_event_index_.has_value()) {
        return;
    }
    Event &event = events_[*pending_event_index_];
    if (event.scheduled_effective_time_ns > timestamp_ns) {
        return;
    }
    const ApplyResult result = apply(event.delta_capacity_bytes, event.scheduled_effective_time_ns);
    event.actual_effective_time_ns = event.scheduled_effective_time_ns;
    event.capacity_before_bytes = result.capacity_before_bytes;
    event.capacity_after_bytes = result.capacity_after_bytes;
    event.status = result.applied ? "APPLIED" : "REJECTED";
    if (!result.reason.empty()) {
        event.reason = result.reason;
    }
    if (result.applied) {
        current_capacity_bytes_ = result.capacity_after_bytes;
        extra_units_ += event.delta_capacity_bytes > 0 ? 1 : -1;
    }
    pending_event_index_.reset();
}

std::optional<int64_t> CacheCapacityAutoscaler::pending_effective_time_ns() const {
    if (!pending_event_index_.has_value()) {
        return std::nullopt;
    }
    return events_[*pending_event_index_].scheduled_effective_time_ns;
}

void CacheCapacityAutoscaler::ExportCsv(const std::string &output_result_path) const {
    std::filesystem::create_directories(output_result_path);
    std::ofstream out(output_result_path + "/cache_capacity_scaling_events.csv");
    if (!out.is_open()) {
        throw std::runtime_error("failed to open cache autoscaling event CSV");
    }
    out << "EventId,Scope,Target,MetricName,MetricMinuteStartNs,MetricMinuteStart,MetricValueSeconds,"
           "EvictedBlocks,Action,TriggerTimeNs,TriggerTime,ScheduledEffectiveTimeNs,ScheduledEffectiveTime,"
           "ActualEffectiveTimeNs,ActualEffectiveTime,DeltaCapacityTiB,CapacityBeforeTiB,CapacityAfterTiB,"
           "Status,Reason\n";
    out << std::fixed << std::setprecision(9);
    for (const auto &event : events_) {
        out << event.event_id << ",group," << target_group_ << ",IdleAfterReuseP10Seconds,"
            << event.metric_minute_start_ns << ',' << FormatTimestamp(event.metric_minute_start_ns) << ','
            << event.metric_value_seconds << ',' << event.evicted_blocks << ',' << event.action << ','
            << event.trigger_time_ns << ',' << FormatTimestamp(event.trigger_time_ns) << ','
            << event.scheduled_effective_time_ns << ',' << FormatTimestamp(event.scheduled_effective_time_ns) << ',';
        if (event.actual_effective_time_ns.has_value()) {
            out << *event.actual_effective_time_ns << ',' << FormatTimestamp(*event.actual_effective_time_ns);
        } else {
            out << ',';
        }
        out << ',' << static_cast<double>(event.delta_capacity_bytes) / kTiB << ','
            << static_cast<double>(event.capacity_before_bytes) / kTiB << ',';
        if (event.actual_effective_time_ns.has_value()) {
            out << static_cast<double>(event.capacity_after_bytes) / kTiB;
        }
        out << ',' << event.status << ',' << event.reason << '\n';
    }
}

} // namespace kv_cache_manager
