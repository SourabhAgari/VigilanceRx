package com.healthcare.rxvigilance.watermark;

import com.healthcare.rxvigilance.config.WatermarkConfig;
import com.healthcare.rxvigilance.domain.Channel;
import com.healthcare.rxvigilance.domain.EventType;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class RxFillWatermarkStrategyTest {

    @Test
    void extractTimestampMapsFillDateToUtcMidnightMillis() {
        WatermarkConfig config = new WatermarkConfig(Duration.ofHours(24), Duration.ofMinutes(5));
        TimestampAssigner<RxFillEvent> timestampAssigner = RxFillWatermarkStrategy
                .create(config)
                .createTimestampAssigner(UnregisteredMetricsGroup::new);

        RxFillEvent event = fillEvent(LocalDate.of(2026, Month.JULY, 20));
        long expected = LocalDate.of(2026, Month.JULY, 20)
                .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertThat(timestampAssigner.extractTimestamp(event, Long.MIN_VALUE)).isEqualTo(expected);
    }

    @Test
    void onPeriodicEmitTrailsMaxSeenTimestampByOutOfOrderness() {
        WatermarkConfig config = new WatermarkConfig(Duration.ofHours(24), Duration.ofMinutes(5));
        WatermarkGenerator<RxFillEvent> generator =
                RxFillWatermarkStrategy.create(config)
                        .createWatermarkGenerator(UnregisteredMetricsGroup::new);
        RecordingWatermarkOutput output = new RecordingWatermarkOutput();

        long day1 = LocalDate.of(2026, Month.JULY, 18).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        long day3 = LocalDate.of(2026, Month.JULY, 20).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

        generator.onEvent(fillEvent(LocalDate.of(2026, Month.JULY, 20)), day3, output);
        generator.onEvent(fillEvent(LocalDate.of(2026, Month.JULY, 18)), day1, output);

        generator.onPeriodicEmit(output);

        assertThat(output.lastWatermarkMillis).isEqualTo(day3 - Duration.ofHours(24).toMillis() - 1);
    }

    @Test
    void onPeriodicEmitMarksIdleAfterConfiguredQuietPeriod() throws InterruptedException {
        WatermarkConfig config = new WatermarkConfig(Duration.ofHours(24), Duration.ofMillis(50));
        WatermarkGenerator<RxFillEvent> generator =
                RxFillWatermarkStrategy.create(config).createWatermarkGenerator(UnregisteredMetricsGroup::new);
        RecordingWatermarkOutput output = new RecordingWatermarkOutput();

        generator.onPeriodicEmit(output);   // starts the inactivity clock; always false here
        Thread.sleep(150);
        generator.onPeriodicEmit(output);

        assertThat(output.idle).isTrue();
    }


    private static RxFillEvent fillEvent(LocalDate fillDate) {
        return new RxFillEvent(
                EventType.FILL, "CLM-1", "MBR-1", "NDC-1", fillDate, 30,
                BigDecimal.valueOf(30), "PHM-1", "RX-1", 3, Channel.RETAIL, null);
    }

    private class RecordingWatermarkOutput implements WatermarkOutput {

        private Long lastWatermarkMillis;
        private boolean idle;

        @Override
        public void emitWatermark(Watermark watermark) {
            lastWatermarkMillis = watermark.getTimestamp();
        }

        @Override
        public void markIdle() {
            idle = true;
        }

        @Override
        public void markActive() {
            idle = false;
        }
    }
}
