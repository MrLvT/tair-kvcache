#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/optimizer/manager/cache_capacity_autoscaler.h"

using namespace kv_cache_manager;

class CacheCapacityAutoscalerTest : public TESTBASE {};

TEST_F(CacheCapacityAutoscalerTest, AppliesDelayedScaleOutAndImmediateScaleIn) {
    OptCacheAutoscalingConfig config;
    config.set_enabled(true);
    config.set_scale_out_threshold_seconds(300.0);
    config.set_scale_in_threshold_seconds(360.0);
    config.set_scale_step_tib(27);
    config.set_scale_out_delay_seconds(300);

    const int64_t baseline = 100LL * CacheCapacityAutoscaler::kTiB;
    CacheCapacityAutoscaler autoscaler(config, "global", baseline);
    CacheRetentionTracker::SummaryRow low;
    low.minute_start_ns = 0;
    low.evicted_blocks = 10;
    low.idle_p10_ns = 299.0 * CacheCapacityAutoscaler::kSecondNs;
    autoscaler.OnMetric(low, 60LL * CacheCapacityAutoscaler::kSecondNs);

    ASSERT_TRUE(autoscaler.has_pending_action());
    EXPECT_EQ(*autoscaler.pending_effective_time_ns(), 360LL * CacheCapacityAutoscaler::kSecondNs);

    int64_t runtime_capacity = baseline;
    auto apply = [&](int64_t delta, int64_t) {
        const int64_t before = runtime_capacity;
        runtime_capacity += delta;
        return CacheCapacityAutoscaler::ApplyResult{true, before, runtime_capacity, "applied"};
    };
    autoscaler.ApplyDueThrough(359LL * CacheCapacityAutoscaler::kSecondNs, apply);
    EXPECT_EQ(runtime_capacity, baseline);
    autoscaler.ApplyDueThrough(360LL * CacheCapacityAutoscaler::kSecondNs, apply);
    EXPECT_EQ(runtime_capacity, baseline + 27LL * CacheCapacityAutoscaler::kTiB);
    EXPECT_EQ(autoscaler.extra_units(), 1);

    CacheRetentionTracker::SummaryRow high;
    high.minute_start_ns = 360LL * CacheCapacityAutoscaler::kSecondNs;
    high.evicted_blocks = 10;
    high.idle_p10_ns = 361.0 * CacheCapacityAutoscaler::kSecondNs;
    autoscaler.OnMetric(high, 420LL * CacheCapacityAutoscaler::kSecondNs);
    autoscaler.ApplyDueThrough(420LL * CacheCapacityAutoscaler::kSecondNs, apply);
    EXPECT_EQ(runtime_capacity, baseline);
    EXPECT_EQ(autoscaler.extra_units(), 0);
}

TEST_F(CacheCapacityAutoscalerTest, IgnoresDeadBandEmptyMinutesAndRepeatedLowSignal) {
    OptCacheAutoscalingConfig config;
    config.set_enabled(true);
    CacheCapacityAutoscaler autoscaler(config, "global", 100LL * CacheCapacityAutoscaler::kTiB);

    CacheRetentionTracker::SummaryRow empty;
    empty.minute_start_ns = 0;
    autoscaler.OnMetric(empty, 60LL * CacheCapacityAutoscaler::kSecondNs);
    EXPECT_FALSE(autoscaler.has_pending_action());

    CacheRetentionTracker::SummaryRow dead_band;
    dead_band.minute_start_ns = 60LL * CacheCapacityAutoscaler::kSecondNs;
    dead_band.evicted_blocks = 1;
    dead_band.idle_p10_ns = 330.0 * CacheCapacityAutoscaler::kSecondNs;
    autoscaler.OnMetric(dead_band, 120LL * CacheCapacityAutoscaler::kSecondNs);
    EXPECT_FALSE(autoscaler.has_pending_action());

    CacheRetentionTracker::SummaryRow low = dead_band;
    low.minute_start_ns = 120LL * CacheCapacityAutoscaler::kSecondNs;
    low.idle_p10_ns = 299.0 * CacheCapacityAutoscaler::kSecondNs;
    autoscaler.OnMetric(low, 180LL * CacheCapacityAutoscaler::kSecondNs);
    ASSERT_TRUE(autoscaler.has_pending_action());
}
