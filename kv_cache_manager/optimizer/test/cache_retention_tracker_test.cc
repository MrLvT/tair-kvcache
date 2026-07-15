#include <filesystem>

#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/optimizer/analysis/tracker/cache_retention_tracker.h"
#include "kv_cache_manager/optimizer/config/optimizer_config.h"
#include "kv_cache_manager/optimizer/config/types.h"

using namespace kv_cache_manager;

class CacheRetentionTrackerTest : public TESTBASE {};

TEST_F(CacheRetentionTrackerTest, TracksPhysicalLifetimeAndLastReadOnly) {
    CacheRetentionTracker tracker(
        std::unordered_map<std::string, std::string>{{"instance_a", "service_x"}});
    BlockEntry reused;
    reused.key = 1;
    BlockEntry never_reused;
    never_reused.key = 2;
    BlockEntry censored;
    censored.key = 3;

    tracker.OnTimestamp("instance_a", 0);
    tracker.OnBlockBirth("instance_a", &reused, 0);
    tracker.OnBlockBirth("instance_a", &never_reused, 10LL * 1000 * 1000 * 1000);
    tracker.OnBlockBirth("instance_a", &censored, 20LL * 1000 * 1000 * 1000);
    tracker.OnBlockReadHit("instance_a", &reused, 20LL * 1000 * 1000 * 1000);
    // A write touch is deliberately not an event consumed by this tracker.
    tracker.OnBlockRetentionEviction("instance_a", &reused, 65LL * 1000 * 1000 * 1000);
    tracker.OnBlockRetentionEviction("instance_a", &never_reused, 65LL * 1000 * 1000 * 1000);
    tracker.Finalize("instance_a", 180LL * 1000 * 1000 * 1000);

    const auto &rows = tracker.InstanceRows("instance_a");
    ASSERT_EQ(rows.size(), 4);
    EXPECT_EQ(rows[0].evicted_blocks, 0);
    EXPECT_EQ(rows[1].evicted_blocks, 2);
    EXPECT_EQ(rows[1].reused_evicted_blocks, 1);
    EXPECT_EQ(rows[1].never_reused_evicted_blocks, 1);
    ASSERT_TRUE(rows[1].lifetime_average_ns.has_value());
    EXPECT_DOUBLE_EQ(*rows[1].lifetime_average_ns, 60.0 * 1000 * 1000 * 1000);
    ASSERT_TRUE(rows[1].lifetime_p50_ns.has_value());
    EXPECT_DOUBLE_EQ(*rows[1].lifetime_p50_ns, 60.0 * 1000 * 1000 * 1000);
    ASSERT_TRUE(rows[1].idle_average_ns.has_value());
    EXPECT_DOUBLE_EQ(*rows[1].idle_average_ns, 45.0 * 1000 * 1000 * 1000);
    EXPECT_EQ(rows[2].evicted_blocks, 0);
    EXPECT_EQ(rows[3].evicted_blocks, 0);
}

TEST_F(CacheRetentionTrackerTest, AggregatesRawSamplesByService) {
    CacheRetentionTracker tracker(std::unordered_map<std::string, std::string>{{"instance_a", "service_x"},
                                                                               {"instance_b", "service_x"}});
    BlockEntry a;
    a.key = 11;
    BlockEntry b;
    b.key = 22;

    tracker.OnTimestamp("instance_a", 0);
    tracker.OnTimestamp("instance_b", 0);
    tracker.OnBlockBirth("instance_a", &a, 0);
    tracker.OnBlockBirth("instance_b", &b, 30LL * 1000 * 1000 * 1000);
    tracker.OnBlockRetentionEviction("instance_a", &a, 65LL * 1000 * 1000 * 1000);
    tracker.OnBlockRetentionEviction("instance_b", &b, 65LL * 1000 * 1000 * 1000);
    tracker.Finalize("instance_a", 65LL * 1000 * 1000 * 1000);
    tracker.Finalize("instance_b", 65LL * 1000 * 1000 * 1000);

    const std::string output = "/tmp/kvcm_cache_retention_tracker_test";
    std::filesystem::remove_all(output);
    OptimizerConfig config;
    config.set_output_result_path(output);
    tracker.Export("instance_a", config);

    const auto &rows = tracker.ServiceRows("service_x");
    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ(rows[1].evicted_blocks, 2);
    ASSERT_TRUE(rows[1].lifetime_p50_ns.has_value());
    EXPECT_DOUBLE_EQ(*rows[1].lifetime_p50_ns, 50.0 * 1000 * 1000 * 1000);
    EXPECT_TRUE(std::filesystem::exists(output + "/service_service_x_cache_retention_by_minute.csv"));
    std::filesystem::remove_all(output);
}
