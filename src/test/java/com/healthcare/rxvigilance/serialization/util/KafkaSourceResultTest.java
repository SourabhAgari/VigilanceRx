package com.healthcare.rxvigilance.serialization.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSourceResultTest {
    @Test
    void successCreatesResultWithValueAndNoFailureInfo() {
        KafkaSourceResult<String> result = KafkaSourceResult.success("hello");

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.value()).isEqualTo("hello");
        assertThat(result.rawBytes()).isNull();
        assertThat(result.errorMessage()).isNull();
    }

    @Test
    void failureCreatesResultWithRawBytesAndErrorMessage() {
        byte[] rawBytes = {1, 2, 3};
        KafkaSourceResult<String> result = KafkaSourceResult.failure(rawBytes, "bad bytes");

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.value()).isNull();
        assertThat(result.rawBytes()).isEqualTo(rawBytes);
        assertThat(result.errorMessage()).isEqualTo("bad bytes");
    }

    @Test
    void equalsIsReflexive() {
        KafkaSourceResult<String> result = KafkaSourceResult.success("hello");

        assertThat(result).isEqualTo(result);
    }

    @Test
    void equalsReturnsFalseForDifferentType() {
        KafkaSourceResult<String> result = KafkaSourceResult.success("hello");

        assertThat(result.equals("not a KafkaSourceResult")).isFalse();
    }

    @Test
    void equalsAndHashCodeCompareRawBytesByContentNotReference() {
        KafkaSourceResult<String> first = KafkaSourceResult.failure(new byte[]{1, 2, 3}, "err");
        KafkaSourceResult<String> second = KafkaSourceResult.failure(new byte[]{1, 2, 3}, "err");

        assertThat(first).isEqualTo(second);
        assertThat(first.hashCode()).hasSameHashCodeAs(second.hashCode());
    }

    @Test
    void toStringIncludesValueRawBytesAndErrorMessage() {
        KafkaSourceResult<String> result = KafkaSourceResult.failure(new byte[]{1, 2, 3}, "bad bytes");

        assertThat(result.toString())
                .contains("bad bytes")
                .contains("[1, 2, 3]");
    }
}
