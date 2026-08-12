FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build

# pom.xml alone first. This layer is reused unless dependencies change,
# so editing Java code does not re-download anything.
COPY pom.xml .
RUN mvn -B dependency:go-offline

# #147: the JSON layout is a Log4j *plugin*, and Flink initialises logging with
# its framework classloader before any user code runs — it never scans the
# shaded job JAR in usrlib. The JAR has to sit in /opt/flink/lib, so pull it
# out as a standalone file here. Placed in this layer because it needs only
# pom.xml, so it caches and does not rerun when Java changes. copy-dependencies
# takes the version from pom.xml, so ${log4j.version} stays the single source.

RUN mvn -B dependency:copy-dependencies \
    -DincludeArtifactIds=log4j-layout-template-json \
    -DoutputDirectory=/build/flinklib

COPY src ./src

# No `clean`: target/ does not exist in a fresh layer.
 # Rename to a fixed filename so the runtime stage carries no version number.
RUN mvn -B package -DskipTests && \
      mv target/rx-vigilance-*.jar /build/rx-vigilance.jar


# ------ runtime stage -----------#
# Pinned to match <flink.version> in pom.xml. A floating 1.18 tag could
# ship a different patch release than the job was built against.
FROM flink:1.18.1-java17

# #147: baked at build time because nothing at runtime knows which commit built
# the image. This is the field that answers "did this start after the last
# deploy?" in one Cloud Logging query. Defaults to "unknown" so a plain
# `docker build` with no --build-arg still works.

ARG GIT_SHA=unknown
ENV RXV_IMAGE_TAG=${GIT_SHA}

COPY --chown=flink:flink --from=builder /build/rx-vigilance.jar /opt/flink/usrlib/rx-vigilance.jar

# #147: loadable by Flink's framework classloader, unlike anything in usrlib.
COPY --chown=flink:flink --from=builder /build/flinklib/log4j-layout-template-json-*.jar /opt/flink/lib/
COPY --chown=flink:flink --from=builder /build/src/main/resources/log4j2-gcp-layout.json /opt/flink/usrlib/
