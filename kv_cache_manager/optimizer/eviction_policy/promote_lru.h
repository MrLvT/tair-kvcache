#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kv_cache_manager/optimizer/config/eviction_config.h"
#include "kv_cache_manager/optimizer/eviction_policy/base.h"
#include "kv_cache_manager/optimizer/eviction_policy/common_structure.h"

namespace kv_cache_manager {

class PromoteLruEvictionPolicy : public EvictionPolicy {
public:
    explicit PromoteLruEvictionPolicy(const std::string &name, const PromoteLruParams &params);
    ~PromoteLruEvictionPolicy() override;

    size_t size() const override { return node_map_.size(); }
    void OnBlockWritten(BlockEntry *block) override;
    void OnNodeWritten(std::vector<BlockEntry *> &blocks) override;
    void OnBlockCopied(BlockEntry *block) override;
    void OnBlockAccessedWithOptions(BlockEntry *block, int64_t timestamp, bool refresh_ttl_on_read) override;
    void OnBlockTouched(BlockEntry *block, int64_t timestamp) override;
    std::vector<BlockEntry *> EvictBlocks(size_t count) override;
    void Clear() override;

private:
    struct PromoteListNode : public LinkedListNode {
        BlockEntry *payload_ = nullptr;
        bool protected_queue = false;
    };

    void OnBlockAccessed(BlockEntry *block, int64_t timestamp) override;
    bool PromoteEnabledForTier(const PromoteLruParams &params) const;
    void InsertNewBlock(BlockEntry *block);
    void RefreshInCurrentQueue(PromoteListNode *node);
    void PromoteOrRefresh(PromoteListNode *node);
    BlockEntry *PopTail(LinkedList &list);
    void ClearListLocations();

    bool promote_enabled_ = true;
    LinkedList probation_list_;
    LinkedList protected_list_;
    std::unordered_map<BlockEntry *, PromoteListNode *> node_map_;
};

} // namespace kv_cache_manager
