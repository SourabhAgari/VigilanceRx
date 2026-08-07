FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build

# pom.xml alone first. This layer is reused unless dependencies change,
# so editing Java code does not re-download anything.
COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src ./src

# No `clean`: target/ does not exist in a fresh layer.
 # Rename to a fixed filename so the runtime stage carries no version number.
RUN mvn -B package -DskipTests && \
      mv target/rx-vigilance-*.jar /build/rx-vigilance.jar


# ------ runtime stage -----------#
# Pinned to match <flink.version> in pom.xml. A floating 1.18 tag could
# ship a different patch release than the job was built against.
FROM flink:1.18.1-java17

COPY --chown=flink:flink --from=builder /build/rx-vigilance.jar /opt/flink/usrlib/rx-vigilance.jar
