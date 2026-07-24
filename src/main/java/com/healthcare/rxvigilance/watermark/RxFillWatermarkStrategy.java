package com.healthcare.rxvigilance.watermark;

import com.healthcare.rxvigilance.domain.RxFillEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;
import java.time.ZoneOffset;

public final class RxFillWatermarkStrategy {
    private RxFillWatermarkStrategy() {
    }

    public static WatermarkStrategy<RxFillEvent> create() {
        return WatermarkStrategy
                .<RxFillEvent>forBoundedOutOfOrderness(Duration.ofHours(24))
                .withTimestampAssigner((event, recordTimestamp) ->
                        event.fillDate().atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli())
                .withIdleness(Duration.ofMinutes(5));
    }
}
