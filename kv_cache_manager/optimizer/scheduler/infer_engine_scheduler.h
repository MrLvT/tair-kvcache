#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kv_cache_manager/optimizer/scheduler/infer_active_window.h"
#include "kv_cache_manager/optimizer/trace_loader/optimizer_schema_trace.h"

namespace kv_cache_manager {

// Callback that predicts prefill duration in nanoseconds given (input_len, cache_hit_len).
// When not set, load_balance scheduling falls back to treating every request as equal cost (1).
using PrefillDurationPredictor = std::function<int64_t(int64_t input_len, int64_t cache_hit_len)>;

class InferEngineScheduler {
public:
    void SetEngineInstanceIds(std::vector<std::string> engine_instance_ids);
    void SetActiveWindows(const std::vector<InferEngineActiveWindow> &active_windows);
    void SetPrefillDurationPredictor(PrefillDurationPredictor predictor);

    void ScheduleTraces(const std::string &strategy, std::vector<std::shared_ptr<OptimizerSchemaTrace>> &traces) const;

    void BuildTraceActiveWindows(const std::vector<std::shared_ptr<OptimizerSchemaTrace>> &traces,
                                 int64_t write_delay_ns,
                                 bool require_known_infer_id);

    [[nodiscard]] std::vector<std::string> ActiveInferIds(const std::vector<std::string> &infer_ids,
                                                          int64_t timestamp_ns) const;
    [[nodiscard]] bool IsInferActiveAt(const std::string &infer_id, int64_t timestamp_ns) const;

    [[nodiscard]] std::string ChoosePrefixHitEngineInstance(
        const std::vector<int64_t> &block_ids,
        int64_t timestamp_ns,
        size_t request_idx,
        const std::function<size_t(const std::string &, const std::vector<int64_t> &, int64_t)> &prefix_match_count)
        const;

    // Choose the engine instance with the least inflight prefill load.
    // After selection, call RecordPrefillStart to register the request's load.
    [[nodiscard]] std::string ChooseLoadBalanceEngineInstance(int64_t timestamp_ns, size_t request_idx);

    // Set the number of concurrent prefill lanes per engine (default 1 = serial).
    // Must be called before RecordPrefillStart.
    void SetConcurrencyPerEngine(int32_t concurrency);

    // Record that a prefill started on the given engine. prefill_duration_ns is the
    // predicted duration; the scheduler uses it to track when the GPU becomes free.
    void RecordPrefillStart(const std::string &engine_instance_id,
                            int64_t start_timestamp_ns,
                            int64_t prefill_duration_ns);

    // Drain completed prefills up to current_timestamp_ns, reducing inflight load.
    void DrainCompletedPrefills(int64_t current_timestamp_ns);

    // Reset all load tracking state (e.g. between runs).
    void ResetLoadState();

    [[nodiscard]] int32_t concurrency_per_engine() const { return concurrency_per_engine_; }

    [[nodiscard]] const std::vector<std::string> &engine_instance_ids() const { return engine_instance_ids_; }
    [[nodiscard]] bool has_active_windows() const { return !active_windows_.empty(); }
    [[nodiscard]] bool has_prefill_duration_predictor() const { return prefill_duration_predictor_ != nullptr; }
    [[nodiscard]] const PrefillDurationPredictor &prefill_duration_predictor() const {
        return prefill_duration_predictor_;
    }

private:
    using TraceSchedulingHandler =
        void (InferEngineScheduler::*)(std::vector<std::shared_ptr<OptimizerSchemaTrace>> &) const;

    [[nodiscard]] static const std::unordered_map<std::string, TraceSchedulingHandler> &TraceSchedulingHandlers();

    void ScheduleRoundRobin(std::vector<std::shared_ptr<OptimizerSchemaTrace>> &traces) const;
    [[nodiscard]] std::vector<std::string> ActiveEngineInstanceIds(int64_t timestamp_ns) const;

    std::vector<std::string> engine_instance_ids_;
    std::unordered_set<std::string> engine_instance_id_set_;
    InferActiveWindowSet active_windows_;

    // Load balance state
    PrefillDurationPredictor prefill_duration_predictor_;
    int32_t concurrency_per_engine_ = 1;

    // Per-engine min-heap of lane completion timestamps.
    // Each engine has exactly concurrency_per_engine_ entries; the top is the earliest-free lane.
    std::unordered_map<std::string, std::priority_queue<int64_t, std::vector<int64_t>, std::greater<int64_t>>>
        engine_lanes_;
};

} // namespace kv_cache_manager
