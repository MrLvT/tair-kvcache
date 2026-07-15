#pragma once
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "kv_cache_manager/optimizer/config/types.h"
#include "kv_cache_manager/optimizer/tier_flow/tier_flow_recorder.h"

namespace kv_cache_manager {

enum class CachePresenceEventKind {
    ENTER = 0,
    LEAVE = 1,
};

enum class CachePresenceRemovalReason {
    NONE = 0,
    CAPACITY = 1,
    SCALE_IN = 2,
    TTL_EXPIRED = 3,
    OTHER = 4,
};

struct CachePresenceEvent {
    CachePresenceEventKind kind = CachePresenceEventKind::ENTER;
    CachePresenceRemovalReason removal_reason = CachePresenceRemovalReason::NONE;
    std::string holder_id;
    int64_t block_key = 0;
    int64_t timestamp_ns = 0;
};

struct GetCacheLocationRes {
    std::string trace_id;
    int64_t kvcm_hit_length;
    std::vector<size_t> hit_indices;
    std::vector<int64_t> evicted_keys;
    std::vector<TierFlowKeyEvent> tier_flow_events;
};

struct WriteCacheRes {
    std::string trace_id;
    int64_t kvcm_write_length;
    int64_t kvcm_write_hit_length;
    std::vector<int64_t> pool_source_write_keys;
    std::vector<int64_t> evicted_keys;
    std::vector<TierFlowKeyEvent> tier_flow_events;
    std::vector<CachePresenceEvent> presence_events;
};

} // namespace kv_cache_manager
