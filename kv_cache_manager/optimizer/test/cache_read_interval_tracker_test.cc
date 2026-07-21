#include <stdexcept>

#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/optimizer/analysis/tracker/cache_read_interval_tracker.h"
#include "kv_cache_manager/optimizer/config/types.h"

using namespace kv_cache_manager;

class CacheReadIntervalTrackerTest : public TESTBASE {};

TEST_F(CacheReadIntervalTrackerTest, TracksConsecutiveReadHitsAndResetsAtRebirth) {
    CacheReadIntervalTracker tracker;
    BlockEntry reused;
    reused.key = 1;
    BlockEntry read_once;
    read_once.key = 2;

    constexpr int64_t second = 1000LL * 1000 * 1000;
    tracker.OnTimestamp("global", 0);
    tracker.OnBlockBirth("global", &reused, 0);
    tracker.OnBlockReadHit("global", &reused, 10 * second);
    tracker.OnBlockReadHit("global", &reused, 20 * second);
    tracker.OnBlockReadHit("global", &reused, 50 * second);

    tracker.OnBlockBirth("global", &read_once, 51 * second);
    tracker.OnBlockReadHit("global", &read_once, 52 * second);
    tracker.OnBlockRetentionEviction("global", &read_once, 59 * second);

    tracker.OnBlockRetentionEviction("global", &reused, 59 * second);
    tracker.OnBlockBirth("global", &reused, 60 * second);
    tracker.OnBlockReadHit("global", &reused, 70 * second);
    tracker.OnBlockReadHit("global", &reused, 80 * second);
    tracker.Finalize("global", 180 * second);

    const auto &rows = tracker.InstanceRows("global");
    ASSERT_EQ(rows.size(), 4);
    EXPECT_EQ(rows[0].interval_samples, 2);
    ASSERT_TRUE(rows[0].average_ns.has_value());
    EXPECT_DOUBLE_EQ(*rows[0].average_ns, 20.0 * second);
    ASSERT_TRUE(rows[0].p50_ns.has_value());
    EXPECT_DOUBLE_EQ(*rows[0].p50_ns, 20.0 * second);
    ASSERT_TRUE(rows[0].p75_ns.has_value());
    EXPECT_DOUBLE_EQ(*rows[0].p75_ns, 25.0 * second);
    ASSERT_TRUE(rows[0].p95_ns.has_value());
    EXPECT_DOUBLE_EQ(*rows[0].p95_ns, 29.0 * second);
    ASSERT_TRUE(rows[0].p99_ns.has_value());
    EXPECT_DOUBLE_EQ(*rows[0].p99_ns, 29.8 * second);

    EXPECT_EQ(rows[1].interval_samples, 1);
    ASSERT_TRUE(rows[1].average_ns.has_value());
    EXPECT_DOUBLE_EQ(*rows[1].average_ns, 10.0 * second);
    EXPECT_EQ(rows[2].interval_samples, 0);
    EXPECT_EQ(rows[3].interval_samples, 0);

    const auto &histogram = tracker.InstanceHistogram("global");
    ASSERT_EQ(histogram.size(), 2);
    EXPECT_EQ(histogram.at(10), 2);
    EXPECT_EQ(histogram.at(30), 1);
}

TEST_F(CacheReadIntervalTrackerTest, UsesOneSecondCeilingHistogramBuckets) {
    CacheReadIntervalTracker tracker;
    BlockEntry block;
    block.key = 1;
    constexpr int64_t second = 1000LL * 1000 * 1000;
    tracker.OnBlockReadHit("global", &block, 0);
    tracker.OnBlockReadHit("global", &block, second / 2);
    tracker.OnBlockReadHit("global", &block, second + second / 2);

    const auto &histogram = tracker.InstanceHistogram("global");
    ASSERT_EQ(histogram.size(), 1);
    EXPECT_EQ(histogram.at(1), 2);
}

TEST_F(CacheReadIntervalTrackerTest, RejectsNonMonotonicReadsForOneBlock) {
    CacheReadIntervalTracker tracker;
    BlockEntry block;
    block.key = 1;
    tracker.OnBlockBirth("global", &block, 0);
    tracker.OnBlockReadHit("global", &block, 20);
    EXPECT_THROW(tracker.OnBlockReadHit("global", &block, 10), std::runtime_error);
}
