package com.healthcare.rxvigilance.logging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.layout.template.json.JsonTemplateLayout;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.util.SortedArrayStringMap;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JsonLogLayoutTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode render(LogEvent event) throws Exception {
        JsonTemplateLayout layout = JsonTemplateLayout.newBuilder()
                .setConfiguration(new DefaultConfiguration())
                .setEventTemplateUri("classpath:log4j2-gcp-layout.json")
                .build();
        return MAPPER.readTree(layout.toSerializable(event));
    }

    private static Log4jLogEvent.Builder event() {
        return Log4jLogEvent.newBuilder()
                .setLoggerName("com.healthcare.rxvigilance.AdherenceJob")
                .setLevel(Level.INFO)
                .setMessage(new SimpleMessage("job started"))
                .setThreadName("main")
                .setTimeMillis(1_700_000_000_000L);
    }

    @Test
    void rendersValidJsonWithTheStandardFields() throws Exception {
        JsonNode node = render(event().build());

        assertThat(node.get("severity").asText()).isEqualTo("INFO");
        assertThat(node.get("message").asText()).isEqualTo("job started");
        assertThat(node.get("logger").asText())
                .isEqualTo("com.healthcare.rxvigilance.AdherenceJob");
        assertThat(node.get("thread").asText()).isEqualTo("main");
        assertThat(node.get("service").asText()).isEqualTo("rx-vigilance");
        assertThat(node.get("jobName").asText()).isEqualTo("adherence-job");
        assertThat(node.hasNonNull("timestamp")).isTrue();
        // Not just non-null: if the env lookup does not resolve, the value ships
        // as the literal "${env:...}" string and the field is useless in prod.
        // Locally the env var is unset, so this is the fallback.
        assertThat(node.get("imageTag").asText())
                .doesNotContain("${")
                .isEqualTo("unknown");
    }

    /**
     * GCP's severity names are not log4j's. Without the map in the template,
     * Cloud Logging files a WARN as DEFAULT severity, and every severity filter
     * in the runbook silently returns nothing.
     */
    @Test
    void mapsLog4jLevelsToCloudLoggingSeverities() throws Exception {
        assertThat(render(event().setLevel(Level.WARN).build()).get("severity").asText())
                .isEqualTo("WARNING");
        assertThat(render(event().setLevel(Level.ERROR).build()).get("severity").asText())
                .isEqualTo("ERROR");
        assertThat(render(event().setLevel(Level.FATAL).build()).get("severity").asText())
                .isEqualTo("EMERGENCY");
    }

    @Test
    void includesClassNameMessageAndStackTraceWhenAnExceptionIsLogged() throws Exception {
        JsonNode node = render(event()
                .setLevel(Level.ERROR)
                .setThrown(new IllegalStateException("sink write failed"))
                .build());

        assertThat(node.at("/exception/className").asText())
                .isEqualTo("java.lang.IllegalStateException");
        assertThat(node.at("/exception/message").asText()).isEqualTo("sink write failed");
        assertThat(node.at("/exception/stackTrace").asText())
                .contains("JsonLogLayoutTest");
    }

    /**
     * #148's logging half puts operatorUid and subtask in the thread context.
     * flatten:true must lift them to top level — otherwise every canned query in
     * the runbook needs an "mdc." prefix that nobody will remember.
     */
    @Test
    void flattensThreadContextToTopLevelFields() throws Exception {
        SortedArrayStringMap context = new SortedArrayStringMap();
        context.putValue("operatorUid", "adherence-process");
        context.putValue("subtask", "0");

        JsonNode node = render(event().setContextData(context).build());

        assertThat(node.get("operatorUid").asText()).isEqualTo("adherence-process");
        assertThat(node.get("subtask").asText()).isEqualTo("0");
    }
}
