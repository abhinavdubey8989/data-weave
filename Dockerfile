# Stage 1: Build the application
FROM gradle:8.10-jdk21 AS builder

WORKDIR /app

# Copy only the necessary Gradle files first (for better caching)
COPY settings.gradle build.gradle gradlew ./
COPY gradle ./gradle

# Copy source code of all modules
COPY domain ./domain
COPY web ./web
COPY consumers ./consumers

# Build the web module (this will also build domain as dependency)
# The bootJar task creates the executable Spring Boot fat JAR
RUN ./gradlew :web:bootJar --no-daemon

# Stage 2: Run the application
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Copy the built JAR from the builder stage
COPY --from=builder /app/web/build/libs/*.jar app.jar

# Expose the port your web module listens on (default 9111 as per your properties)
EXPOSE 9111

# Set environment variables that your application expects (e.g., SERIALIZATION_TYPE)
# You can override these at runtime
ENV SERIALIZATION_TYPE=json \
    SPRING_PROFILES_ACTIVE=default

# Healthcheck (optional)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:9111/actuator/health || exit 1

# Run the Spring Boot application
ENTRYPOINT ["java", "-jar", "app.jar"]