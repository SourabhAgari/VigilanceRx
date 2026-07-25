package com.healthcare.rxvigilance.watermark;

import com.healthcare.rxvigilance.config.WatermarkConfig;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.ZoneOffset;

public final class RxFillWatermarkStrategy {
    private RxFillWatermarkStrategy() {
    }

    public static WatermarkStrategy<RxFillEvent> create(WatermarkConfig config) {
        return WatermarkStrategy
                .<RxFillEvent>forBoundedOutOfOrderness(config.outOfOrderness())
                .withTimestampAssigner((event, recordTimestamp) ->
                        event.fillDate().atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli())
                .withIdleness(config.idleness());
    }
}
