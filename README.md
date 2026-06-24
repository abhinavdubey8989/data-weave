## DataWeave

A multi-module Java 21 Spring Boot application having REST APIs, Kafka producers, Kafka consumers.

### 1. Project Structure

```text
dataweave
│
├── build.gradle
├── settings.gradle
│
├── web
│   ├── build.gradle
│   └── src/
│
├── domain
│   ├── build.gradle
│   └── src/
│
└── consumers
    ├── build.gradle
    └── src/
```

## 2. Modules

### 2.1 domain

Shared module containing:

* DTOs
* POJOs
* Kafka event models

This module is intended to be dependency-only and is consumed by other modules.

### 2.2 web

Spring Boot application responsible for:

* REST APIs
* Kafka producer logic
* Request validation
* API controllers

Depends on:

```gradle
implementation project(':domain')
```

### 2.3 consumers

Spring Boot application responsible for:

* Kafka consumers
* Event processing
* Downstream integrations

Depends on:

```gradle
implementation project(':domain')
```



## 3. Technology Stack

| Technology  | Version           |
|-------------|-------------------|
| Java        | 21                |
| jenv        | 0.5.9             |
| Spring Boot | 4.0.6             |
| Gradle      | Wrapper Managed   |
| Lombok      | Latest Compatible |
| JUnit       | 5.10.2            |


## 4. Java version management
- Used jenv for java multiple version management locally
- Below are useful commands

```bash
# check jenv version
jenv --version

# get list of java version on local machine
jenv versions

# switch to a particular java version
jenv local 21


# check java version for this project
java -version
```


## 5 Useful Gradle Commands


### 5.1 Show All Projects

- Displays all modules discovered by Gradle.

```bash
./gradlew projects
```

Expected output:

```text
Root project 'dataweave'

+--- Project ':web'
+--- Project ':domain'
\--- Project ':consumers'
```


### 5.2 Clean Build Artifacts
- Removes `build/` directories from all modules.
- Useful before performing a fresh build.

```bash
./gradlew clean
```


### 5.3 Build Project

Build all modules:
- Compiles all modules
- Runs all tests
- Creates JAR files
- Verifies dependencies

```bash
#  Build all modules
./gradlew build

#  Build specific module
./gradlew :web:build
./gradlew :consumers:build
./gradlew :domain:build
```


### 5.4 Compile Source Code
- Compiles Java source files without running tests.

```bash
./gradlew compileJava
```


### 5.5 Run Tests
- Executes unit tests across all modules.

```bash
# For all modules
./gradlew test

# For Specific Module
./gradlew :web:test
./gradlew :consumers:test
./gradlew :domain:test
```


### 5.6 View Dependencies

- Useful for troubleshooting dependency conflicts.

```bash
# Entire Project
./gradlew dependencies

# Specific Module
./gradlew :web:dependencies
./gradlew :domain:dependencies
./gradlew :consumers:dependencies

```


### 5.7 Refresh Dependencies

```bash
./gradlew --refresh-dependencies build
```

- Forces Gradle to re-download dependencies.
- Useful when snapshots or repository artifacts have changed.



## 6. Building Deployable JARs

- The `domain` module is generally a library module and is not deployed independently
- The deployable JAR is typically generated from Spring Boot modules:

```text
- web
- consumers
```


### 6.1 Build All Deployable JARs

```bash
# Generated artifacts:
#    - web/build/libs/...
#    - consumers/build/libs/..
./gradlew clean build


# Build Only Web JAR
# Generated artifacts: web/build/libs/web-0.0.1-SNAPSHOT.jar
./gradlew :web:bootJar


# Build Only Consumers JAR
# Generated artifacts: consumers/build/libs/consumers-0.0.1-SNAPSHOT.jar
./gradlew :consumers:bootJar

```


## 7. Running Deployable JARs



```bash
# Run Web Application
java -jar web/build/libs/web-0.0.1-SNAPSHOT.jar

# Run Consumers Application
java -jar consumers/build/libs/consumers-0.0.1-SNAPSHOT.jar

```



## 8. Development Workflow

```bash
# Clean Existing Artifacts
./gradlew clean

# Run Unit Tests
./gradlew test

# Build Everything
./gradlew build

# Generate Deployable JARs
./gradlew :web:bootJar :consumers:bootJar

# Run Applications
java -jar web/build/libs/*.jar
java -jar consumers/build/libs/*.jar
```
