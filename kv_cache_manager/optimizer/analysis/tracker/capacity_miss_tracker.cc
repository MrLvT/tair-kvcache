#include "kv_cache_manager/optimizer/analysis/tracker/capacity_miss_tracker.h"

#include <algorithm>
#include <deque>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>

namespace kv_cache_manager {
namespace {

constexpr int64_t kNsPerSecond = 1000000000LL;

int64_t SecondsToNs(int64_t seconds) {
    if (seconds <= 0 || seconds > std::numeric_limits<int64_t>::max() / kNsPerSecond) {
        throw std::invalid_argument("capacity miss duration seconds is out of range");
    }
    return seconds * kNsPerSecond;
}

bool IsGhostEligible(CachePresenceRemovalReason reason) {
    return reason == CachePresenceRemovalReason::CAPACITY || reason == CachePresenceRemovalReason::SCALE_IN;
}

} // namespace

CapacityMissTracker::CapacityMissTracker(int64_t ghost_retention_seconds, int64_t window_seconds)
    : ghost_retention_ns_(SecondsToNs(ghost_retention_seconds)), window_ns_(SecondsToNs(window_seconds)) {
    if (ghost_retention_ns_ < window_ns_) {
        throw std::invalid_argument("capacity miss ghost retention must be no shorter than the metric window");
    }
}

void CapacityMissTracker::AddLive(const std::string &scope_id,
                                  const std::string &holder_id,
                                  int64_t block_key,
                                  int64_t /*timestamp_ns*/) {
    directory_[scope_id][block_key].live_holders.insert(holder_id);
}

void CapacityMissTracker::RemoveLive(const std::string &scope_id,
                                     const std::string &holder_id,
                                     int64_t block_key,
                                     int64_t timestamp_ns,
                                     CachePresenceRemovalReason reason) {
    auto scope_it = directory_.find(scope_id);
    if (scope_it == directory_.end()) {
        return;
    }
    auto block_it = scope_it->second.find(block_key);
    if (block_it == scope_it->second.end()) {
        return;
    }
    auto &state = block_it->second;
    if (state.live_holders.erase(holder_id) == 0) {
        return;
    }
    if (!state.live_holders.empty()) {
        return;
    }
    if (IsGhostEligible(reason)) {
        state.ghost = GhostEntry{timestamp_ns, reason};
        state.has_ghost = true;
    } else {
        state.has_ghost = false;
    }
}

void CapacityMissTracker::ApplyEvents(const std::string &scope_id,
                                      const std::vector<CachePresenceEvent> &events) {
    for (const auto &event : events) {
        if (event.kind == CachePresenceEventKind::ENTER) {
            AddLive(scope_id, event.holder_id, event.block_key, event.timestamp_ns);
        } else {
            RemoveLive(
                scope_id, event.holder_id, event.block_key, event.timestamp_ns, event.removal_reason);
        }
    }
}

bool CapacityMissTracker::HasValidGhost(BlockState *state, int64_t timestamp_ns) const {
    if (state == nullptr || !state->has_ghost) {
        return false;
    }
    if (timestamp_ns < state->ghost.evicted_at_ns || timestamp_ns - state->ghost.evicted_at_ns > ghost_retention_ns_) {
        state->has_ghost = false;
        return false;
    }
    return true;
}

CapacityMissPrefixSnapshot CapacityMissTracker::SnapshotPrefix(const std::string &scope_id,
                                                               const std::vector<int64_t> &block_ids,
                                                               int64_t timestamp_ns) {
    CapacityMissPrefixSnapshot snapshot;
    auto scope_it = directory_.find(scope_id);
    if (scope_it == directory_.end()) {
        return snapshot;
    }

    bool global_contiguous = true;
    bool counterfactual_contiguous = true;
    for (const int64_t key : block_ids) {
        auto state_it = scope_it->second.find(key);
        BlockState *state = state_it == scope_it->second.end() ? nullptr : &state_it->second;
        const bool live = state != nullptr && !state->live_holders.empty();
        const bool ghost = !live && HasValidGhost(state, timestamp_ns);
        if (global_contiguous && live) {
            ++snapshot.global_prefix_blocks;
        } else {
            global_contiguous = false;
        }
        if (counterfactual_contiguous && (live || ghost)) {
            ++snapshot.counterfactual_prefix_blocks;
        } else {
            counterfactual_contiguous = false;
        }
        if (!global_contiguous && !counterfactual_contiguous) {
            break;
        }
    }
    return snapshot;
}

CapacityMissRecord CapacityMissTracker::RecordRequest(const std::string &scope_id,
                                                       const std::string &engine_instance_id,
                                                       const std::string &trace_id,
                                                       int64_t timestamp_ns,
                                                       size_t block_size_tokens,
                                                       size_t cacheable_blocks,
                                                       size_t actual_prefix_blocks,
                                                       const CapacityMissPrefixSnapshot &snapshot) {
    CapacityMissRecord record;
    record.timestamp_ns = timestamp_ns;
    record.trace_id = trace_id;
    record.engine_instance_id = engine_instance_id;
    record.scope_id = scope_id;
    record.cacheable_prompt_tokens = cacheable_blocks * block_size_tokens;
    record.actual_prefix_tokens = std::min(actual_prefix_blocks, cacheable_blocks) * block_size_tokens;
    record.global_prefix_tokens = std::min(snapshot.global_prefix_blocks, cacheable_blocks) * block_size_tokens;
    record.counterfactual_prefix_tokens =
        std::min(snapshot.counterfactual_prefix_blocks, cacheable_blocks) * block_size_tokens;
    record.routing_miss_tokens = record.global_prefix_tokens > record.actual_prefix_tokens
                                     ? record.global_prefix_tokens - record.actual_prefix_tokens
                                     : 0;
    record.capacity_miss_tokens = record.counterfactual_prefix_tokens - record.global_prefix_tokens;
    record.cold_miss_tokens = record.cacheable_prompt_tokens - record.counterfactual_prefix_tokens;
    records_.push_back(record);
    return record;
}

void CapacityMissTracker::ExportCsv(const std::string &output_result_path) const {
    std::filesystem::create_directories(output_result_path);
    const std::string filename = output_result_path + "/hierarchical_capacity_miss.csv";
    std::ofstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open capacity miss CSV: " + filename);
    }
    file << "TimestampNs,TraceId,EngineInstanceId,StoragePoolId,CacheablePromptTokens,ActualPrefixTokens,"
            "GlobalPrefixTokens,CounterfactualPrefixTokens,RoutingMissTokens,CapacityMissTokens,ColdMissTokens,"
            "AccCacheablePromptTokens,AccRoutingMissTokens,AccCapacityMissTokens,AccColdMissTokens,"
            "CapacityMissTps5m,CapacityMissRatio5m\n";

    size_t acc_cacheable = 0;
    size_t acc_routing = 0;
    size_t acc_capacity = 0;
    size_t acc_cold = 0;
    size_t window_cacheable = 0;
    size_t window_capacity = 0;
    std::deque<const CapacityMissRecord *> window;
    const int64_t first_timestamp = records_.empty() ? 0 : records_.front().timestamp_ns;
    const double window_seconds = static_cast<double>(window_ns_) / kNsPerSecond;

    for (const auto &record : records_) {
        acc_cacheable += record.cacheable_prompt_tokens;
        acc_routing += record.routing_miss_tokens;
        acc_capacity += record.capacity_miss_tokens;
        acc_cold += record.cold_miss_tokens;
        window.push_back(&record);
        window_cacheable += record.cacheable_prompt_tokens;
        window_capacity += record.capacity_miss_tokens;
        while (!window.empty() && window.front()->timestamp_ns <= record.timestamp_ns - window_ns_) {
            window_cacheable -= window.front()->cacheable_prompt_tokens;
            window_capacity -= window.front()->capacity_miss_tokens;
            window.pop_front();
        }

        file << record.timestamp_ns << "," << record.trace_id << "," << record.engine_instance_id << ","
             << record.scope_id << "," << record.cacheable_prompt_tokens << "," << record.actual_prefix_tokens << ","
             << record.global_prefix_tokens << "," << record.counterfactual_prefix_tokens << ","
             << record.routing_miss_tokens << "," << record.capacity_miss_tokens << "," << record.cold_miss_tokens
             << "," << acc_cacheable << "," << acc_routing << "," << acc_capacity << "," << acc_cold << ",";
        if (record.timestamp_ns - first_timestamp >= window_ns_) {
            file << static_cast<double>(window_capacity) / window_seconds << ","
                 << (window_cacheable > 0 ? static_cast<double>(window_capacity) / window_cacheable : 0.0);
        } else {
            file << ",";
        }
        file << "\n";
    }
}

} // namespace kv_cache_manager
