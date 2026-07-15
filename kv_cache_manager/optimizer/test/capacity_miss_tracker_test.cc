#include <filesystem>
#include <fstream>
#include <string>

#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/optimizer/analysis/tracker/capacity_miss_tracker.h"

using namespace kv_cache_manager;

class CapacityMissTrackerTest : public TESTBASE {};

TEST_F(CapacityMissTrackerTest, ClassifiesColdRoutingAndCapacityMiss) {
    CapacityMissTracker tracker(30, 5);
    const std::vector<int64_t> keys{1, 2};

    auto cold_snapshot = tracker.SnapshotPrefix("model", keys, 1);
    auto cold = tracker.RecordRequest("model", "engine_b", "cold", 1, 16, keys.size(), 0, cold_snapshot);
    EXPECT_EQ(cold.capacity_miss_tokens, 0);
    EXPECT_EQ(cold.routing_miss_tokens, 0);
    EXPECT_EQ(cold.cold_miss_tokens, 32);

    tracker.AddLive("model", "engine:engine_a", 1, 2);
    auto routing_snapshot = tracker.SnapshotPrefix("model", keys, 3);
    auto routing = tracker.RecordRequest("model", "engine_b", "routing", 3, 16, keys.size(), 0, routing_snapshot);
    EXPECT_EQ(routing.routing_miss_tokens, 16);
    EXPECT_EQ(routing.capacity_miss_tokens, 0);
    EXPECT_EQ(routing.cold_miss_tokens, 16);

    tracker.RemoveLive("model", "engine:engine_a", 1, 4, CachePresenceRemovalReason::CAPACITY);
    auto capacity_snapshot = tracker.SnapshotPrefix("model", keys, 5);
    auto capacity = tracker.RecordRequest("model", "engine_b", "capacity", 5, 16, keys.size(), 0, capacity_snapshot);
    EXPECT_EQ(capacity.routing_miss_tokens, 0);
    EXPECT_EQ(capacity.capacity_miss_tokens, 16);
    EXPECT_EQ(capacity.cold_miss_tokens, 16);
}

TEST_F(CapacityMissTrackerTest, CreatesGhostOnlyAfterLastReplicaDisappears) {
    CapacityMissTracker tracker(30, 5);
    tracker.AddLive("model", "engine:a", 7, 1);
    tracker.AddLive("model", "pool:model", 7, 1);
    tracker.RemoveLive("model", "engine:a", 7, 2, CachePresenceRemovalReason::CAPACITY);

    auto live = tracker.SnapshotPrefix("model", {7}, 3);
    EXPECT_EQ(live.global_prefix_blocks, 1);
    EXPECT_EQ(live.counterfactual_prefix_blocks, 1);

    tracker.RemoveLive("model", "pool:model", 7, 4, CachePresenceRemovalReason::CAPACITY);
    auto ghost = tracker.SnapshotPrefix("model", {7}, 5);
    EXPECT_EQ(ghost.global_prefix_blocks, 0);
    EXPECT_EQ(ghost.counterfactual_prefix_blocks, 1);
}

TEST_F(CapacityMissTrackerTest, StopsAtFirstColdGapAndExpiresGhost) {
    constexpr int64_t kSecond = 1000000000LL;
    CapacityMissTracker tracker(10, 5);
    tracker.AddLive("model", "engine:a", 1, 0);
    tracker.AddLive("model", "engine:a", 2, 0);
    tracker.RemoveLive("model", "engine:a", 2, kSecond, CachePresenceRemovalReason::SCALE_IN);
    tracker.AddLive("model", "engine:a", 4, 0);
    tracker.RemoveLive("model", "engine:a", 4, kSecond, CachePresenceRemovalReason::CAPACITY);

    auto gap = tracker.SnapshotPrefix("model", {1, 2, 3, 4}, 2 * kSecond);
    EXPECT_EQ(gap.global_prefix_blocks, 1);
    EXPECT_EQ(gap.counterfactual_prefix_blocks, 2);

    auto expired = tracker.SnapshotPrefix("model", {2}, 12 * kSecond);
    EXPECT_EQ(expired.global_prefix_blocks, 0);
    EXPECT_EQ(expired.counterfactual_prefix_blocks, 0);
}

TEST_F(CapacityMissTrackerTest, ExportsOnlyCompleteMetricWindows) {
    constexpr int64_t kSecond = 1000000000LL;
    CapacityMissTracker tracker(10, 5);
    tracker.AddLive("model", "engine:a", 1, 0);
    tracker.RemoveLive("model", "engine:a", 1, 0, CachePresenceRemovalReason::CAPACITY);
    auto first_snapshot = tracker.SnapshotPrefix("model", {1}, 0);
    tracker.RecordRequest("model", "engine:a", "first", 0, 10, 1, 0, first_snapshot);
    auto second_snapshot = tracker.SnapshotPrefix("model", {1}, 5 * kSecond);
    tracker.RecordRequest("model", "engine:a", "second", 5 * kSecond, 10, 1, 0, second_snapshot);

    const std::string output = GetTestTempRootPath() + "/capacity_miss_tracker";
    std::filesystem::remove_all(output);
    tracker.ExportCsv(output);
    std::ifstream file(output + "/hierarchical_capacity_miss.csv");
    ASSERT_TRUE(file.is_open());
    std::string header;
    std::string first;
    std::string second;
    ASSERT_TRUE(static_cast<bool>(std::getline(file, header)));
    ASSERT_TRUE(static_cast<bool>(std::getline(file, first)));
    ASSERT_TRUE(static_cast<bool>(std::getline(file, second)));
    EXPECT_NE(header.find("CapacityMissTps5m"), std::string::npos);
    EXPECT_NE(first.find(",,"), std::string::npos);
    EXPECT_NE(second.find(",2,"), std::string::npos); // 10 tokens / 5 seconds.
}
