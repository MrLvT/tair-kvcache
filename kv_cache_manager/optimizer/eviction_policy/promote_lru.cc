#include "kv_cache_manager/optimizer/eviction_policy/promote_lru.h"

#include <algorithm>

namespace kv_cache_manager {

PromoteLruEvictionPolicy::PromoteLruEvictionPolicy(const std::string &name, const PromoteLruParams &params)
    : EvictionPolicy(name), promote_enabled_(PromoteEnabledForTier(params)) {}

PromoteLruEvictionPolicy::~PromoteLruEvictionPolicy() {
    probation_list_.clear();
    protected_list_.clear();
    node_map_.clear();
}

bool PromoteLruEvictionPolicy::PromoteEnabledForTier(const PromoteLruParams &params) const {
    if (params.enabled_tiers.empty()) {
        return true;
    }
    return std::find(params.enabled_tiers.begin(), params.enabled_tiers.end(), name()) != params.enabled_tiers.end();
}

void PromoteLruEvictionPolicy::InsertNewBlock(BlockEntry *block) {
    if (block == nullptr) {
        return;
    }
    auto it = node_map_.find(block);
    if (it != node_map_.end()) {
        RefreshInCurrentQueue(it->second);
        return;
    }

    auto *node = new PromoteListNode();
    node->payload_ = block;
    node->protected_queue = !promote_enabled_;
    if (node->protected_queue) {
        protected_list_.push_front(node);
    } else {
        probation_list_.push_front(node);
    }
    node_map_[block] = node;
}

void PromoteLruEvictionPolicy::OnBlockWritten(BlockEntry *block) { InsertNewBlock(block); }

void PromoteLruEvictionPolicy::OnNodeWritten(std::vector<BlockEntry *> &blocks) {
    for (auto *block : blocks) {
        OnBlockWritten(block);
    }
}

void PromoteLruEvictionPolicy::OnBlockCopied(BlockEntry *block) { InsertNewBlock(block); }

void PromoteLruEvictionPolicy::RefreshInCurrentQueue(PromoteListNode *node) {
    if (node == nullptr) {
        return;
    }
    if (node->protected_queue) {
        protected_list_.move_to_front(node);
    } else {
        probation_list_.move_to_front(node);
    }
}

void PromoteLruEvictionPolicy::PromoteOrRefresh(PromoteListNode *node) {
    if (node == nullptr) {
        return;
    }
    if (!promote_enabled_ || node->protected_queue) {
        RefreshInCurrentQueue(node);
        return;
    }

    probation_list_.unlink(node);
    node->protected_queue = true;
    protected_list_.push_front(node);
}

void PromoteLruEvictionPolicy::OnBlockAccessed(BlockEntry *block, int64_t timestamp) {
    OnBlockAccessedWithOptions(block, timestamp, true);
}

void PromoteLruEvictionPolicy::OnBlockAccessedWithOptions(BlockEntry *block,
                                                          int64_t timestamp,
                                                          bool refresh_ttl_on_read) {
    (void)timestamp;
    (void)refresh_ttl_on_read;
    auto it = node_map_.find(block);
    if (it == node_map_.end()) {
        return;
    }
    PromoteOrRefresh(it->second);
}

void PromoteLruEvictionPolicy::OnBlockTouched(BlockEntry *block, int64_t timestamp) {
    (void)timestamp;
    auto it = node_map_.find(block);
    if (it == node_map_.end()) {
        return;
    }
    RefreshInCurrentQueue(it->second);
}

BlockEntry *PromoteLruEvictionPolicy::PopTail(LinkedList &list) {
    auto *node = static_cast<PromoteListNode *>(list.getTail());
    if (node == nullptr) {
        return nullptr;
    }
    BlockEntry *block = node->payload_;
    list.unlink(node);
    node_map_.erase(block);
    ClearBlockLocation(block);
    delete node;
    return block;
}

std::vector<BlockEntry *> PromoteLruEvictionPolicy::EvictBlocks(size_t count) {
    std::vector<BlockEntry *> evicted;
    evicted.reserve(std::min(count, node_map_.size()));
    while (evicted.size() < count && !probation_list_.empty()) {
        BlockEntry *block = PopTail(probation_list_);
        if (block != nullptr) {
            evicted.push_back(block);
        }
    }
    while (evicted.size() < count && !protected_list_.empty()) {
        BlockEntry *block = PopTail(protected_list_);
        if (block != nullptr) {
            evicted.push_back(block);
        }
    }
    return evicted;
}

void PromoteLruEvictionPolicy::ClearListLocations() {
    for (auto &[block, node] : node_map_) {
        (void)node;
        ClearBlockLocation(block);
    }
}

void PromoteLruEvictionPolicy::Clear() {
    ClearListLocations();
    probation_list_.clear();
    protected_list_.clear();
    node_map_.clear();
}

} // namespace kv_cache_manager
