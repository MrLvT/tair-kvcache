#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kv_cache_manager/optimizer/config/insight_simulator_types.h"

namespace kv_cache_manager {

struct CapacityMissPrefixSnapshot {
    size_t global_prefix_blocks = 0;
    size_t counterfactual_prefix_blocks = 0;
};

struct CapacityMissRecord {
    int64_t timestamp_ns = 0;
    std::string trace_id;
    std::string engine_instance_id;
    std::string scope_id;
    size_t cacheable_prompt_tokens = 0;
    size_t actual_prefix_tokens = 0;
    size_t global_prefix_tokens = 0;
    size_t counterfactual_prefix_tokens = 0;
    size_t routing_miss_tokens = 0;
    size_t capacity_miss_tokens = 0;
    size_t cold_miss_tokens = 0;
};

class CapacityMissTracker {
public:
    CapacityMissTracker(int64_t ghost_retention_seconds, int64_t window_seconds);

    void AddLive(const std::string &scope_id,
                 const std::string &holder_id,
                 int64_t block_key,
                 int64_t timestamp_ns);
    void RemoveLive(const std::string &scope_id,
                    const std::string &holder_id,
                    int64_t block_key,
                    int64_t timestamp_ns,
                    CachePresenceRemovalReason reason);
    void ApplyEvents(const std::string &scope_id, const std::vector<CachePresenceEvent> &events);

    [[nodiscard]] CapacityMissPrefixSnapshot SnapshotPrefix(const std::string &scope_id,
                                                            const std::vector<int64_t> &block_ids,
                                                            int64_t timestamp_ns);
    CapacityMissRecord RecordRequest(const std::string &scope_id,
                                     const std::string &engine_instance_id,
                                     const std::string &trace_id,
                                     int64_t timestamp_ns,
                                     size_t block_size_tokens,
                                     size_t cacheable_blocks,
                                     size_t actual_prefix_blocks,
                                     const CapacityMissPrefixSnapshot &snapshot);
    void ExportCsv(const std::string &output_result_path) const;

    [[nodiscard]] const std::vector<CapacityMissRecord> &records() const { return records_; }

private:
    struct GhostEntry {
        int64_t evicted_at_ns = 0;
        CachePresenceRemovalReason reason = CachePresenceRemovalReason::NONE;
    };

    struct BlockState {
        std::unordered_set<std::string> live_holders;
        GhostEntry ghost;
        bool has_ghost = false;
    };

    bool HasValidGhost(BlockState *state, int64_t timestamp_ns) const;

    int64_t ghost_retention_ns_ = 0;
    int64_t window_ns_ = 0;
    std::unordered_map<std::string, std::unordered_map<int64_t, BlockState>> directory_;
    std::vector<CapacityMissRecord> records_;
};

} // namespace kv_cache_manager
