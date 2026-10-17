# Build context contains only the selected reviewed executable JAR.
ARG JAVA_IMAGE=eclipse-temurin:21.0.12.1_1-jre-noble
FROM ${JAVA_IMAGE}
ARG WAREXIS_BUILD=p11-14fa97e6
LABEL org.opencontainers.image.title="Warexis backend" \
      org.opencontainers.image.version="${WAREXIS_BUILD}"
WORKDIR /app
COPY --chown=10001:10001 application.jar /app/application.jar
USER 10001:10001
ENTRYPOINT ["java", "-jar", "/app/application.jar"]
