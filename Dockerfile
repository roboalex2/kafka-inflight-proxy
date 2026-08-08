
# Build stage
FROM maven:3.9.16-eclipse-temurin-25-alpine AS build
WORKDIR /workspace

COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 \
    mvn --batch-mode --no-transfer-progress dependency:go-offline

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 \
    mvn --batch-mode --no-transfer-progress clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:25-jre-alpine AS runtime

RUN addgroup --system app && adduser --system --ingroup app app
WORKDIR /app

COPY --from=build --chown=app:app \
    /workspace/target/kafka-inflight-proxy-*.jar /app/kafka-inflight-proxy.jar

USER app

EXPOSE 8080 19092

ENTRYPOINT ["java", "-jar", "/app/kafka-inflight-proxy.jar"]
