FROM maven:3.9-eclipse-temurin AS BUILDER

WORKDIR /build
COPY pom.xml .

# download the dependency but it will be stored inside
# will be donloaded in /root/.m2/repository/
RUN mvn -B dependency:go-offline

# copy src folder into src folder inside build folder
COPY src ./src

# below command creates a fat jar or uber jar inside target folder under
# build directory
RUN mvn clean package -DskipTests

# ------ runtime stage -----------#
FROM flink:1.18

COPY --from=BUILDER /build/target/rx-vigilance-1.0.0-SNAPSHOT.jar /opt/flink/usrlib/rx-vigilance.jar
