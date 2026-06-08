#include "kv_cache_manager/optimizer/manager/optimizer_runner.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstddef>
#include <limits>
#include <stdexcept>
#include <utility>
#include <variant>

#include "kv_cache_manager/common/logger.h"
#include "kv_cache_manager/optimizer/config/optimizer_config.h"
#include "kv_cache_manager/optimizer/manager/optimizer_loader.h"
#include "kv_cache_manager/optimizer/trace_loader/standard_trace_loader.h"

namespace kv_cache_manager {
namespace {
int64_t TtlUsToNs(int64_t ttl_us) { return ttl_us > 0 ? ttl_us * 1000 : ttl_us; }

[[noreturn]] void LogAndThrowReplayError(const std::string &message) {
    KVCM_LOG_ERROR("%s", message.c_str());
    throw std::runtime_error(message);
}

size_t ValidateFullBlockTrace(const GetLocationSchemaTrace &trace, size_t block_size) {
    if (block_size == 0) {
        LogAndThrowReplayError("GetCacheLocation requires positive instance block_size");
    }

    const size_t input_tokens = trace.input_token_count();
    const size_t max_full_blocks = input_tokens / block_size;
    if (trace.keys().size() <= max_full_blocks) {
        return input_tokens;
    }

    LogAndThrowReplayError(
        "GetCacheLocation trace contains partial tail block keys: instance_id=" + trace.instance_id() +
        ", trace_id=" + trace.trace_id() + ", keys=" + std::to_string(trace.keys().size()) +
        ", input_len=" + std::to_string(input_tokens) + ", block_size=" + std::to_string(block_size) +
        ", max_full_blocks=" + std::to_string(max_full_blocks) +
        ". Standard optimizer traces must drop incomplete tail blocks before replay.");
}

size_t CeilDiv(size_t lhs, size_t rhs) { return rhs == 0 ? 0 : (lhs + rhs - 1) / rhs; }

int64_t SaturatingAdd(int64_t lhs, int64_t rhs) {
    if (rhs > 0 && lhs > std::numeric_limits<int64_t>::max() - rhs) {
        return std::numeric_limits<int64_t>::max();
    }
    return lhs + rhs;
}

uint64_t StableHashString(const std::string &value, uint64_t seed) {
    uint64_t hash = 1469598103934665603ULL ^ seed;
    for (unsigned char c : value) {
        hash ^= static_cast<uint64_t>(c);
        hash *= 1099511628211ULL;
    }
    return hash;
}
} // namespace

void OptimizerRunner::Run(OptimizerConfig &config) {
    ConfigureReplay(config);

    const char *stream_trace = std::getenv("KVCM_OPTIMIZER_STREAM_TRACE");
    if (stream_trace != nullptr && std::string(stream_trace) != "0") {
        auto starting_time = std::chrono::high_resolution_clock::now();
        size_t trace_count = 0;
        StandardTraceLoader::ForEachFromFile(
            config.trace_file_path(), [this, &trace_count](const std::shared_ptr<OptimizerSchemaTrace> &trace) {
                ReplayTraceWithPendingWrites(trace);
                trace_count++;
            });
        FlushAllPendingEvents();
        auto ending_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ending_time - starting_time).count();
        KVCM_LOG_INFO(
            "Streamed and replayed %zu traces from file: %s in %ld ms",
            trace_count,
            config.trace_file_path().c_str(),
            duration);
        return;
    }

    auto starting_time = std::chrono::high_resolution_clock::now();
    auto traces = OptimizerLoader::LoadTrace(config);
    auto ending_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ending_time - starting_time).count();
    KVCM_LOG_INFO(
        "Loaded %zu traces from file: %s in %ld ms", traces.size(), config.trace_file_path().c_str(), duration);

    starting_time = std::chrono::high_resolution_clock::now();
    RunTraces(traces);
    ending_time = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(ending_time - starting_time).count();
    KVCM_LOG_INFO("Playback traces in %ld ms", duration);
}

void OptimizerRunner::RunTraces(const std::vector<std::shared_ptr<OptimizerSchemaTrace>> &traces) {
    pending_writes_ = {};
    pending_scheduler_releases_ = {};
    next_pending_write_sequence_ = 0;
    next_pending_release_sequence_ = 0;
    scheduler_states_.clear();
    queued_requests_.clear();
    for (const auto &trace : traces) {
        ReplayTraceWithPendingWrites(trace);
    }
    FlushAllPendingEvents();
}

void OptimizerRunner::ConfigureReplay(const OptimizerConfig &config) {
    write_delay_ns_ = config.trace_replay_config().write_delay_ns();
    if (write_delay_ns_ <= 0) {
        LogAndThrowReplayError("trace_replay.write_delay_ns must be positive");
    }
    compute_time_config_ = config.trace_replay_config().compute_time_config();
    pending_writes_ = {};
    pending_scheduler_releases_ = {};
    next_pending_write_sequence_ = 0;
    next_pending_release_sequence_ = 0;
    scheduler_states_.clear();
    queued_requests_.clear();
}

void OptimizerRunner::ReplayTraceWithPendingWrites(const std::shared_ptr<OptimizerSchemaTrace> &trace) {
    if (!trace) {
        return;
    }
    FlushPendingEventsThrough(trace->timestamp_ns());
    RunTrace(trace);
}

void OptimizerRunner::RunTrace(std::shared_ptr<OptimizerSchemaTrace> trace) {
    if (!trace) {
        return;
    }

    if (auto request_trace = std::dynamic_pointer_cast<RequestSchemaTrace>(trace)) {
        HandleRequest(*request_trace);
    } else if (auto get_trace = std::dynamic_pointer_cast<GetLocationSchemaTrace>(trace)) {
        if (get_trace->query_type() != "prefix_match") {
            KVCM_LOG_WARN("Unsupported query type: %s", get_trace->query_type().c_str());
            return;
        }
        HandleGetLocation(*get_trace);
    } else if (auto write_trace = std::dynamic_pointer_cast<WriteCacheSchemaTrace>(trace)) {
        HandleWriteCache(*write_trace);
        stats_collector_->UpdateTimestamp(write_trace->instance_id(), write_trace->timestamp_ns());
    } else {
        KVCM_LOG_WARN("Unknown trace type, skipping");
    }
}

std::shared_ptr<RadixTreeIndex> OptimizerRunner::GetIndexer(const std::string &instance_id) {
    auto indexer = indexer_manager_->GetOptIndexer(instance_id);
    if (!indexer) {
        KVCM_LOG_ERROR("Optimizer indexer not found for instance_id: %s", instance_id.c_str());
    }
    return indexer;
}

void OptimizerRunner::HandleRequest(const RequestSchemaTrace &trace) {
    if (trace.query_type() != "prefix_match") {
        KVCM_LOG_WARN("Unsupported query type: %s", trace.query_type().c_str());
        return;
    }
    if (compute_time_config_.enabled()) {
        HandleQueuedRequest(trace);
    } else {
        HandleFixedDelayRequest(trace);
    }
}

void OptimizerRunner::HandleFixedDelayRequest(const RequestSchemaTrace &trace) {
    HandleGetLocation(trace);
    ScheduleRequestWrite(trace, SaturatingAdd(trace.timestamp_ns(), write_delay_ns_), SchedulerKeyForInstance(trace.instance_id()));
}

void OptimizerRunner::HandleQueuedRequest(const RequestSchemaTrace &trace) {
    const std::string scheduler_key = SchedulerKeyForInstance(trace.instance_id());
    auto &queue = queued_requests_[scheduler_key];
    auto &state = GetSchedulerState(scheduler_key);
    if (queue.empty() && state.available_lanes > 0) {
        StartQueuedRequest(trace, trace.timestamp_ns(), scheduler_key);
        return;
    }
    queue.push_back(trace);
}

void OptimizerRunner::StartQueuedRequest(const RequestSchemaTrace &trace,
                                         int64_t start_timestamp_ns,
                                         const std::string &scheduler_key) {
    RequestTiming timing;
    timing.has_timing = true;
    timing.arrival_timestamp_ns = trace.timestamp_ns();
    timing.start_timestamp_ns = start_timestamp_ns;
    timing.queue_delay_ns = start_timestamp_ns > trace.timestamp_ns() ? start_timestamp_ns - trace.timestamp_ns() : 0;

    const auto read_result = ReplayGetLocationAt(trace, start_timestamp_ns, &timing);
    if (!read_result.ok) {
        TryStartNextQueuedRequest(scheduler_key, start_timestamp_ns);
        return;
    }

    auto &state = GetSchedulerState(scheduler_key);
    if (state.available_lanes == 0) {
        LogAndThrowReplayError("scheduler has no available execution lane for key=" + scheduler_key);
    }
    state.available_lanes--;
    ScheduleSchedulerRelease(timing.finish_timestamp_ns, scheduler_key);
    ScheduleRequestWrite(trace, timing.finish_timestamp_ns, scheduler_key);
}

void OptimizerRunner::TryStartNextQueuedRequest(const std::string &scheduler_key, int64_t timestamp_ns) {
    auto queue_it = queued_requests_.find(scheduler_key);
    if (queue_it == queued_requests_.end() || queue_it->second.empty()) {
        return;
    }
    auto &state = GetSchedulerState(scheduler_key);
    while (!queue_it->second.empty() && state.available_lanes > 0) {
        auto trace = queue_it->second.front();
        queue_it->second.pop_front();
        const int64_t start_timestamp_ns = std::max(timestamp_ns, trace.timestamp_ns());
        StartQueuedRequest(trace, start_timestamp_ns, scheduler_key);
    }
}

void OptimizerRunner::ScheduleRequestWrite(const RequestSchemaTrace &trace,
                                           int64_t write_timestamp_ns,
                                           const std::string &scheduler_key) {
    if (write_timestamp_ns < trace.timestamp_ns()) {
        LogAndThrowReplayError("request write timestamp overflows int64: instance_id=" + trace.instance_id() +
                               ", trace_id=" + trace.trace_id());
    }

    WriteCacheSchemaTrace write_trace;
    write_trace.set_instance_id(trace.instance_id());
    write_trace.set_trace_id(trace.trace_id() + ":write");
    write_trace.set_timestamp_ns(write_timestamp_ns);
    write_trace.set_keys(trace.keys());
    write_trace.set_ttl_us(trace.ttl_us());
    pending_writes_.push(
        PendingWrite{write_trace.timestamp_ns(), next_pending_write_sequence_++, scheduler_key, std::move(write_trace)});
}

void OptimizerRunner::ScheduleSchedulerRelease(int64_t timestamp_ns, const std::string &scheduler_key) {
    if (scheduler_key.empty()) {
        return;
    }
    pending_scheduler_releases_.push(
        PendingSchedulerRelease{timestamp_ns, next_pending_release_sequence_++, scheduler_key});
}

void OptimizerRunner::FlushPendingEventsThrough(int64_t timestamp_ns) {
    while (true) {
        const bool has_write = !pending_writes_.empty() && pending_writes_.top().timestamp_ns <= timestamp_ns;
        const bool has_release =
            !pending_scheduler_releases_.empty() && pending_scheduler_releases_.top().timestamp_ns <= timestamp_ns;
        if (!has_write && !has_release) {
            return;
        }
        if (has_write &&
            (!has_release || pending_writes_.top().timestamp_ns <= pending_scheduler_releases_.top().timestamp_ns)) {
            auto pending = pending_writes_.top();
            pending_writes_.pop();
            RunPendingWrite(pending);
        } else {
            auto pending = pending_scheduler_releases_.top();
            pending_scheduler_releases_.pop();
            RunPendingSchedulerRelease(pending);
        }
    }
}

void OptimizerRunner::FlushAllPendingEvents() {
    while (!pending_writes_.empty() || !pending_scheduler_releases_.empty()) {
        const bool run_write = !pending_writes_.empty() &&
                               (pending_scheduler_releases_.empty() ||
                                pending_writes_.top().timestamp_ns <= pending_scheduler_releases_.top().timestamp_ns);
        if (run_write) {
            auto pending = pending_writes_.top();
            pending_writes_.pop();
            RunPendingWrite(pending);
        } else {
            auto pending = pending_scheduler_releases_.top();
            pending_scheduler_releases_.pop();
            RunPendingSchedulerRelease(pending);
        }
    }
}

void OptimizerRunner::RunPendingWrite(const PendingWrite &pending_write) {
    HandleWriteCache(pending_write.trace);
    stats_collector_->UpdateTimestamp(pending_write.trace.instance_id(), pending_write.trace.timestamp_ns());
    if (compute_time_config_.enabled() && !pending_write.scheduler_key.empty()) {
        TryStartNextQueuedRequest(pending_write.scheduler_key, pending_write.timestamp_ns);
    }
}

void OptimizerRunner::RunPendingSchedulerRelease(const PendingSchedulerRelease &pending_release) {
    auto &state = GetSchedulerState(pending_release.scheduler_key);
    if (state.available_lanes < state.total_lanes) {
        state.available_lanes++;
    }
    TryStartNextQueuedRequest(pending_release.scheduler_key, pending_release.timestamp_ns);
}

std::string OptimizerRunner::SchedulerKeyForInstance(const std::string &instance_id) const {
    if (!compute_time_config_.queue_by_instance_group()) {
        return instance_id;
    }
    auto group_it = instance_group_names_.find(instance_id);
    if (group_it == instance_group_names_.end() || group_it->second.empty()) {
        return instance_id;
    }
    return group_it->second;
}

OptimizerRunner::SchedulerState &OptimizerRunner::GetSchedulerState(const std::string &scheduler_key) {
    auto &state = scheduler_states_[scheduler_key];
    if (!state.initialized) {
        state.total_lanes = SchedulerLaneCountForKey(scheduler_key);
        state.available_lanes = state.total_lanes;
        state.initialized = true;
    }
    return state;
}

size_t OptimizerRunner::SchedulerLaneCountForKey(const std::string &scheduler_key) const {
    const auto primary = compute_time_config_.scheduler_lane_count();
    const auto alternate = compute_time_config_.scheduler_lane_count_alternate();
    if (alternate > 0 && (StableHashString(scheduler_key, compute_time_config_.noise_seed()) & 1ULL) != 0) {
        return static_cast<size_t>(alternate);
    }
    return static_cast<size_t>(primary > 0 ? primary : 1);
}

int64_t OptimizerRunner::SampleNoiseOffsetNs(const std::string &trace_id) const {
    const auto &offsets = compute_time_config_.noise_offsets_ns();
    const auto &weights = compute_time_config_.noise_weights();
    if (offsets.empty()) {
        return 0;
    }

    double weight_sum = 0.0;
    for (auto weight : weights) {
        weight_sum += weight;
    }
    if (weight_sum <= 0.0) {
        return 0;
    }

    const auto hash = StableHashString(trace_id, compute_time_config_.noise_seed());
    const double unit = static_cast<double>(hash >> 11) * (1.0 / 9007199254740992.0);
    const double target = unit * weight_sum;
    double cumulative = 0.0;
    for (size_t i = 0; i < offsets.size(); ++i) {
        cumulative += weights[i];
        if (target <= cumulative) {
            return offsets[i];
        }
    }
    return offsets.back();
}

int64_t OptimizerRunner::ComputeServiceTimeNs(const ReadReplayResult &read_result, const std::string &trace_id) const {
    const auto miss_blocks = read_result.simulated_missed_blocks;
    const auto hit_blocks = read_result.block_size_tokens == 0 ? 0 : read_result.simulated_hit_tokens / read_result.block_size_tokens;
    const __int128 position_sum =
        static_cast<__int128>(miss_blocks) * static_cast<__int128>(hit_blocks) +
        (miss_blocks > 0 ? (static_cast<__int128>(miss_blocks) * static_cast<__int128>(miss_blocks - 1)) / 2 : 0);

    const auto &config = compute_time_config_;
    const __int128 service_time =
        static_cast<__int128>(config.base_latency_ns()) +
        static_cast<__int128>(config.miss_block_latency_ns()) * static_cast<__int128>(miss_blocks) +
        static_cast<__int128>(config.miss_block_position_latency_ns()) * position_sum +
        static_cast<__int128>(config.latency_offset_ns()) + static_cast<__int128>(SampleNoiseOffsetNs(trace_id));
    if (service_time <= 0) {
        return 1;
    }
    if (service_time > std::numeric_limits<int64_t>::max()) {
        return std::numeric_limits<int64_t>::max();
    }
    return static_cast<int64_t>(service_time);
}

void OptimizerRunner::SubmitReadRecord(const std::string &instance_id,
                                       const std::string &trace_id,
                                       const std::vector<int64_t> &keys,
                                       int64_t timestamp_ns,
                                       const QueryHit &query_hit,
                                       const std::shared_ptr<RadixTreeIndex> &indexer,
                                       size_t local_read_block_num,
                                       size_t remote_read_block_num,
                                       size_t input_tokens,
                                       size_t block_size_tokens,
                                       const RequestTiming *timing) {
    ReadRecord record{};
    record.timestamp_ns = timestamp_ns;
    record.trace_id = trace_id;
    record.keys_ptr = &keys;
    record.current_cache_blocks = eviction_manager_->GetCurrentInstanceUsage(instance_id);

    auto indexer_map = indexer_manager_->GetAllOptIndexers();
    record.blocks_per_instance.resize(indexer_map.size(), 0);
    size_t idx = 0;
    for (const auto &pair : indexer_map) {
        record.blocks_per_instance[idx] = eviction_manager_->GetCurrentInstanceUsage(pair.first);
        idx++;
    }

    record.remote_hit_blocks = query_hit.remote_hit_block_num;
    record.local_hit_blocks = query_hit.local_hit_block_num;
    record.per_tier_hit_blocks = query_hit.per_tier_hit_block_num;
    record.input_tokens = input_tokens;
    record.block_size_tokens = block_size_tokens;
    record.tier_names = indexer->GetTierNames();
    record.per_tier_blocks = eviction_manager_->GetCurrentInstanceUsagePerTier(instance_id);
    record.local_read_blocks = local_read_block_num;
    record.remote_read_blocks = remote_read_block_num;
    if (timing != nullptr && timing->has_timing) {
        record.has_replay_timing = true;
        record.arrival_timestamp_ns = timing->arrival_timestamp_ns;
        record.start_timestamp_ns = timing->start_timestamp_ns;
        record.finish_timestamp_ns = timing->finish_timestamp_ns;
        record.queue_delay_ns = timing->queue_delay_ns;
        record.service_time_ns = timing->service_time_ns;
        record.simulated_hit_tokens = timing->simulated_hit_tokens;
        record.simulated_missed_blocks = timing->simulated_missed_blocks;
    }

    stats_collector_->OnReadComplete(instance_id, record);
}

void OptimizerRunner::HandleGetLocation(const GetLocationSchemaTrace &trace) {
    ReplayGetLocationAt(trace, trace.timestamp_ns());
}

OptimizerRunner::ReadReplayResult OptimizerRunner::ReplayGetLocationAt(const GetLocationSchemaTrace &trace,
                                                                        int64_t timestamp_ns,
                                                                        RequestTiming *timing) {
    std::string instance_id = trace.instance_id();
    auto indexer = GetIndexer(instance_id);
    if (!indexer) {
        return {};
    }

    const size_t block_size = indexer_manager_->GetInstanceBlockSize(instance_id);
    const size_t input_tokens = ValidateFullBlockTrace(trace, block_size);

    // 读请求前统一清理过期 block，并做节点清理（TTL 使用逻辑过期时刻记录）
    auto expired_evicted_blocks = indexer_manager_->EvictExpiredBeforeAccess(instance_id, timestamp_ns);
    indexer_manager_->CleanEvictedBlocks(expired_evicted_blocks, timestamp_ns, true);

    bool refresh_ttl_on_read = true;
    auto it = instance_ttl_refresh_on_read_.find(instance_id);
    if (it != instance_ttl_refresh_on_read_.end()) {
        refresh_ttl_on_read = it->second;
    }

    QueryHit query_hit;
    const bool read_triggered_tier_write =
        indexer->PrefixQuery(trace.keys(), trace.block_mask(), timestamp_ns, &query_hit, refresh_ttl_on_read);
    if (read_triggered_tier_write) {
        auto capacity_evicted_blocks = indexer_manager_->CheckAndEvict(instance_id, timestamp_ns);
        indexer_manager_->CleanEvictedBlocks(capacity_evicted_blocks, timestamp_ns);
    }

    size_t local_read_block_num = 0;
    if (std::holds_alternative<BlockMaskVector>(trace.block_mask())) {
        const auto &mask_vector = std::get<BlockMaskVector>(trace.block_mask());
        const size_t n = std::min(mask_vector.size(), trace.keys().size());
        local_read_block_num = std::count(mask_vector.begin(), mask_vector.begin() + n, true);
    } else if (std::holds_alternative<BlockMaskOffset>(trace.block_mask())) {
        local_read_block_num = std::min(std::get<BlockMaskOffset>(trace.block_mask()), trace.keys().size());
    }
    size_t remote_read_block_num = trace.keys().size() - local_read_block_num;
    const size_t hit_blocks = query_hit.local_hit_block_num + query_hit.remote_hit_block_num;
    const size_t hit_tokens = std::min(input_tokens, hit_blocks * block_size);
    const size_t missed_tokens = input_tokens > hit_tokens ? input_tokens - hit_tokens : 0;
    const size_t missed_blocks = CeilDiv(missed_tokens, block_size);

    if (timing != nullptr && timing->has_timing) {
        timing->simulated_hit_tokens = hit_tokens;
        timing->simulated_missed_blocks = missed_blocks;
        timing->service_time_ns =
            ComputeServiceTimeNs(
                ReadReplayResult{true, input_tokens, block_size, hit_blocks, hit_tokens, missed_blocks},
                trace.trace_id());
        timing->finish_timestamp_ns = SaturatingAdd(timestamp_ns, timing->service_time_ns);
    }

    SubmitReadRecord(instance_id,
                     trace.trace_id(),
                     trace.keys(),
                     timestamp_ns,
                     query_hit,
                     indexer,
                     local_read_block_num,
                     remote_read_block_num,
                     input_tokens,
                     block_size,
                     timing != nullptr && timing->has_timing ? timing : nullptr);
    stats_collector_->UpdateTimestamp(instance_id, timestamp_ns);
    return ReadReplayResult{true, input_tokens, block_size, hit_blocks, hit_tokens, missed_blocks};
}

void OptimizerRunner::HandleWriteCache(const WriteCacheSchemaTrace &trace) {
    std::string instance_id = trace.instance_id();
    auto indexer = GetIndexer(instance_id);
    if (!indexer) {
        return;
    }

    // 写请求前统一清理过期 block，并做节点清理（TTL 使用逻辑过期时刻记录）
    auto expired_evicted_blocks = indexer_manager_->EvictExpiredBeforeAccess(instance_id, trace.timestamp_ns());
    indexer_manager_->CleanEvictedBlocks(expired_evicted_blocks, trace.timestamp_ns(), true);

    int64_t effective_ttl_ns = TtlUsToNs(trace.ttl_us());
    auto ttl_disabled_it = instance_group_ttl_disabled_.find(instance_id);
    if (ttl_disabled_it != instance_group_ttl_disabled_.end() && ttl_disabled_it->second) {
        effective_ttl_ns = -1;
    }

    auto result = indexer->InsertOnly(trace.keys(), trace.timestamp_ns(), effective_ttl_ns);
    auto capacity_evicted_blocks = indexer_manager_->CheckAndEvict(instance_id, trace.timestamp_ns());
    indexer_manager_->CleanEvictedBlocks(capacity_evicted_blocks, trace.timestamp_ns());
    bool evicted = !capacity_evicted_blocks.empty();
    if (evicted) {
        KVCM_LOG_DEBUG("Eviction at ts=%lld for instance_id: %s",
                       static_cast<long long>(trace.timestamp_ns()),
                       instance_id.c_str());
    }

    WriteRecord record;
    record.timestamp_ns = trace.timestamp_ns();
    record.write_blocks = trace.keys().size();
    record.newly_inserted_blocks = result.inserted_keys.size();
    record.trace_id = trace.trace_id();
    stats_collector_->OnWriteComplete(instance_id, record);
}
} // namespace kv_cache_manager
