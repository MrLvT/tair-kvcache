#pragma once
#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kv_cache_manager/optimizer/analysis/stats_collector.h"
#include "kv_cache_manager/optimizer/config/optimizer_config.h"
#include "kv_cache_manager/optimizer/manager/eviction_manager.h"
#include "kv_cache_manager/optimizer/manager/cache_capacity_autoscaler.h"
#include "kv_cache_manager/optimizer/manager/indexer_manager.h"
#include "kv_cache_manager/optimizer/trace_loader/optimizer_schema_trace.h"

namespace kv_cache_manager {
class OptimizerRunner {
public:
    explicit OptimizerRunner(const std::shared_ptr<OptIndexerManager> &indexer_manager,
                             const std::shared_ptr<OptEvictionManager> &eviction_manager,
                             const std::shared_ptr<StatsCollector> &stats_collector,
                             const std::unordered_map<std::string, bool> &instance_group_ttl_disabled,
                             const std::unordered_map<std::string, bool> &instance_ttl_refresh_on_read,
                             const OptMambaStateConfig &mamba_state_config = OptMambaStateConfig(),
                             CacheRetentionTracker *cache_retention_tracker = nullptr,
                             const OptCacheAutoscalingConfig &cache_autoscaling_config = OptCacheAutoscalingConfig(),
                             const std::string &autoscaling_group_name = "",
                             int64_t autoscaling_baseline_capacity_bytes = 0,
                             const std::string &lru_snapshot_instance_id = "")
        : indexer_manager_(indexer_manager)
        , eviction_manager_(eviction_manager)
        , stats_collector_(stats_collector)
        , instance_group_ttl_disabled_(instance_group_ttl_disabled)
        , instance_ttl_refresh_on_read_(instance_ttl_refresh_on_read)
        , mamba_state_config_(mamba_state_config)
        , cache_retention_tracker_(cache_retention_tracker)
        , autoscaling_group_name_(autoscaling_group_name)
        , lru_snapshot_instance_id_(lru_snapshot_instance_id) {
        if (cache_autoscaling_config.enabled()) {
            cache_capacity_autoscaler_ = std::make_unique<CacheCapacityAutoscaler>(
                cache_autoscaling_config, autoscaling_group_name, autoscaling_baseline_capacity_bytes);
        }
    };
    ~OptimizerRunner() = default;
    void Run(OptimizerConfig &config);
    void RunTraces(const std::vector<std::shared_ptr<OptimizerSchemaTrace>> &traces);
    void RunTrace(std::shared_ptr<OptimizerSchemaTrace> trace);

public:
    ReadRecord HandleGetLocation(const GetLocationSchemaTrace &trace,
                                 bool touch_local_hits = true,
                                 bool local_hits_are_reads = true);
    WriteRecord HandleWriteCache(const WriteCacheSchemaTrace &trace);
    WriteRecord HandleFillCachePath(const WriteCacheSchemaTrace &trace,
                                    const std::vector<size_t> &materialized_indices);
    void ClearMambaState(const std::string &instance_id);
    void ClearAllMambaStates();
    void ExportAutoscalingEvents(const std::string &output_result_path) const;

private:
    struct PrefixSignature {
        size_t length = 0;
        uint64_t hash1 = 0;
        uint64_t hash2 = 0;

        bool operator==(const PrefixSignature &other) const {
            return length == other.length && hash1 == other.hash1 && hash2 == other.hash2;
        }
    };

    struct PrefixSignatureHash {
        size_t operator()(const PrefixSignature &sig) const {
            return static_cast<size_t>(sig.hash1 ^
                                       (sig.hash2 + 0x9e3779b97f4a7c15ULL + (sig.hash1 << 6) + (sig.hash1 >> 2)) ^
                                       static_cast<uint64_t>(sig.length));
        }
    };

    struct MambaStateReadStats {
        bool enabled = false;
        size_t raw_remote_hit_blocks = 0;
        size_t raw_local_hit_blocks = 0;
        size_t candidate_blocks = 0;
        size_t hit_blocks = 0;
        size_t stored_checkpoints = 0;
        size_t bytes_per_state = 0;
    };

    struct MambaStateWriteStats {
        bool enabled = false;
        size_t write_checkpoints = 0;
        size_t new_checkpoints = 0;
        size_t bytes_per_state = 0;
    };

    struct PendingWrite {
        int64_t timestamp_ns = 0;
        uint64_t sequence = 0;
        WriteCacheSchemaTrace trace;
    };

    struct PendingWriteCompare {
        bool operator()(const PendingWrite &lhs, const PendingWrite &rhs) const {
            if (lhs.timestamp_ns != rhs.timestamp_ns) {
                return lhs.timestamp_ns > rhs.timestamp_ns;
            }
            return lhs.sequence > rhs.sequence;
        }
    };

    std::shared_ptr<RadixTreeIndex> GetIndexer(const std::string &instance_id);
    void ReplayTraceWithPendingWrites(const std::shared_ptr<OptimizerSchemaTrace> &trace);
    void HandleRequest(const RequestSchemaTrace &trace);
    void ScheduleRequestWrite(const RequestSchemaTrace &trace);
    void FlushPendingWritesThrough(int64_t timestamp_ns);
    void FlushAllPendingWrites();
    void RunPendingWrite(const WriteCacheSchemaTrace &trace);
    void AdvanceAutoscalingThrough(int64_t timestamp_ns);
    std::vector<PrefixSignature> BuildPrefixSignatures(const std::vector<int64_t> &keys) const;
    std::vector<size_t> MambaCheckpointIndices(size_t key_count) const;
    MambaStateReadStats
    ApplyMambaStateRead(const std::string &instance_id, const std::vector<int64_t> &keys, QueryHit *query_hit);
    MambaStateWriteStats ApplyMambaStateWrite(const std::string &instance_id,
                                              const std::vector<int64_t> &keys,
                                              const std::vector<size_t> *materialized_indices);
    WriteRecord HandleCacheInsert(const WriteCacheSchemaTrace &trace,
                                  bool count_new_tier_write_touch,
                                  const std::vector<size_t> *materialized_indices);
    ReadRecord SubmitReadRecord(const std::string &instance_id,
                                const std::string &trace_id,
                                const std::vector<int64_t> &keys,
                                int64_t timestamp_ns,
                                const QueryHit &query_hit,
                                const std::shared_ptr<RadixTreeIndex> &indexer,
                                size_t local_read_block_num,
                                size_t remote_read_block_num,
                                size_t input_tokens,
                                size_t block_size_tokens,
                                const MambaStateReadStats &mamba_state_stats);

    std::shared_ptr<OptIndexerManager> indexer_manager_;
    std::shared_ptr<OptEvictionManager> eviction_manager_;
    std::shared_ptr<StatsCollector> stats_collector_;
    std::unordered_map<std::string, bool> instance_group_ttl_disabled_;
    std::unordered_map<std::string, bool> instance_ttl_refresh_on_read_;
    OptMambaStateConfig mamba_state_config_;
    std::unordered_map<std::string, std::unordered_set<PrefixSignature, PrefixSignatureHash>> mamba_state_checkpoints_;
    int64_t write_delay_ns_ = 1;
    uint64_t next_pending_write_sequence_ = 0;
    std::priority_queue<PendingWrite, std::vector<PendingWrite>, PendingWriteCompare> pending_writes_;
    CacheRetentionTracker *cache_retention_tracker_ = nullptr;
    std::string autoscaling_group_name_;
    std::unique_ptr<CacheCapacityAutoscaler> cache_capacity_autoscaler_;
    std::string lru_snapshot_instance_id_;
    std::optional<int64_t> next_lru_snapshot_boundary_ns_;
    std::optional<int64_t> last_replay_timestamp_ns_;
};
} // namespace kv_cache_manager
