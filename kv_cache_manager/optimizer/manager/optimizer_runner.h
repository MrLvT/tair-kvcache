#pragma once
#include <cstdint>
#include <deque>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "kv_cache_manager/optimizer/analysis/stats_collector.h"
#include "kv_cache_manager/optimizer/config/optimizer_config.h"
#include "kv_cache_manager/optimizer/manager/eviction_manager.h"
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
                             const std::unordered_map<std::string, std::string> &instance_group_names)
        : indexer_manager_(indexer_manager)
        , eviction_manager_(eviction_manager)
        , stats_collector_(stats_collector)
        , instance_group_ttl_disabled_(instance_group_ttl_disabled)
        , instance_ttl_refresh_on_read_(instance_ttl_refresh_on_read)
        , instance_group_names_(instance_group_names){};
    ~OptimizerRunner() = default;
    void Run(OptimizerConfig &config);
    void RunTraces(const std::vector<std::shared_ptr<OptimizerSchemaTrace>> &traces);
    void RunTrace(std::shared_ptr<OptimizerSchemaTrace> trace);

public:
    void HandleGetLocation(const GetLocationSchemaTrace &trace);
    void HandleWriteCache(const WriteCacheSchemaTrace &trace);

private:
    struct PendingWrite {
        int64_t timestamp_ns = 0;
        uint64_t sequence = 0;
        std::string scheduler_key;
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

    struct PendingSchedulerRelease {
        int64_t timestamp_ns = 0;
        uint64_t sequence = 0;
        std::string scheduler_key;
    };

    struct PendingSchedulerReleaseCompare {
        bool operator()(const PendingSchedulerRelease &lhs, const PendingSchedulerRelease &rhs) const {
            if (lhs.timestamp_ns != rhs.timestamp_ns) {
                return lhs.timestamp_ns > rhs.timestamp_ns;
            }
            return lhs.sequence > rhs.sequence;
        }
    };

    struct RequestTiming {
        bool has_timing = false;
        int64_t arrival_timestamp_ns = 0;
        int64_t start_timestamp_ns = 0;
        int64_t finish_timestamp_ns = 0;
        int64_t queue_delay_ns = 0;
        int64_t service_time_ns = 0;
        size_t simulated_hit_tokens = 0;
        size_t simulated_missed_blocks = 0;
    };

    struct SchedulerState {
        size_t total_lanes = 1;
        size_t available_lanes = 1;
        bool initialized = false;
    };

    struct ReadReplayResult {
        bool ok = false;
        size_t input_tokens = 0;
        size_t block_size_tokens = 0;
        size_t hit_blocks = 0;
        size_t simulated_hit_tokens = 0;
        size_t simulated_missed_blocks = 0;
    };

    std::shared_ptr<RadixTreeIndex> GetIndexer(const std::string &instance_id);
    void ReplayTraceWithPendingWrites(const std::shared_ptr<OptimizerSchemaTrace> &trace);
    void ConfigureReplay(const OptimizerConfig &config);
    void HandleRequest(const RequestSchemaTrace &trace);
    void HandleFixedDelayRequest(const RequestSchemaTrace &trace);
    void HandleQueuedRequest(const RequestSchemaTrace &trace);
    void StartQueuedRequest(const RequestSchemaTrace &trace,
                            int64_t start_timestamp_ns,
                            const std::string &scheduler_key);
    void TryStartNextQueuedRequest(const std::string &scheduler_key, int64_t timestamp_ns);
    void ScheduleRequestWrite(const RequestSchemaTrace &trace,
                              int64_t write_timestamp_ns,
                              const std::string &scheduler_key);
    void ScheduleSchedulerRelease(int64_t timestamp_ns, const std::string &scheduler_key);
    void FlushPendingEventsThrough(int64_t timestamp_ns);
    void FlushAllPendingEvents();
    void RunPendingWrite(const PendingWrite &pending_write);
    void RunPendingSchedulerRelease(const PendingSchedulerRelease &pending_release);
    std::string SchedulerKeyForInstance(const std::string &instance_id) const;
    SchedulerState &GetSchedulerState(const std::string &scheduler_key);
    size_t SchedulerLaneCountForKey(const std::string &scheduler_key) const;
    int64_t ComputeServiceTimeNs(const ReadReplayResult &read_result, const std::string &trace_id) const;
    int64_t SampleNoiseOffsetNs(const std::string &trace_id) const;
    ReadReplayResult ReplayGetLocationAt(const GetLocationSchemaTrace &trace,
                                         int64_t timestamp_ns,
                                         RequestTiming *timing = nullptr);
    void SubmitReadRecord(const std::string &instance_id,
                          const std::string &trace_id,
                          const std::vector<int64_t> &keys,
                          int64_t timestamp_ns,
                          const QueryHit &query_hit,
                          const std::shared_ptr<RadixTreeIndex> &indexer,
                          size_t local_read_block_num,
                          size_t remote_read_block_num,
                          size_t input_tokens,
                          size_t block_size_tokens,
                          const RequestTiming *timing = nullptr);

    std::shared_ptr<OptIndexerManager> indexer_manager_;
    std::shared_ptr<OptEvictionManager> eviction_manager_;
    std::shared_ptr<StatsCollector> stats_collector_;
    std::unordered_map<std::string, bool> instance_group_ttl_disabled_;
    std::unordered_map<std::string, bool> instance_ttl_refresh_on_read_;
    std::unordered_map<std::string, std::string> instance_group_names_;
    OptComputeTimeConfig compute_time_config_;
    int64_t write_delay_ns_ = 1;
    uint64_t next_pending_write_sequence_ = 0;
    uint64_t next_pending_release_sequence_ = 0;
    std::priority_queue<PendingWrite, std::vector<PendingWrite>, PendingWriteCompare> pending_writes_;
    std::priority_queue<PendingSchedulerRelease,
                        std::vector<PendingSchedulerRelease>,
                        PendingSchedulerReleaseCompare>
        pending_scheduler_releases_;
    std::unordered_map<std::string, SchedulerState> scheduler_states_;
    std::unordered_map<std::string, std::deque<RequestSchemaTrace>> queued_requests_;
};
} // namespace kv_cache_manager
