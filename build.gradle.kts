description = "A Kafka Connect SMT for removing nested fields in keys and values."

plugins {
    `java-library`
    id("com.bakdata.release") version "1.7.1"
    id("com.bakdata.sonar") version "1.7.1"
    id("com.bakdata.sonatype") version "1.9.0"
    id("io.freefair.lombok") version "8.12.2"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

group = "com.bakdata.kafka"
repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}

dependencies {
    val kafkaVersion: String by project
    compileOnly(group = "org.apache.kafka", name = "connect-transforms", version = kafkaVersion)
    compileOnly(group = "org.apache.kafka", name = "connect-runtime", version = kafkaVersion) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }

    val log4jVersion: String by project
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)
    testImplementation(group = "org.apache.kafka", name = "connect-api", version = kafkaVersion)
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = "3.3.0") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    testImplementation(group = "org.apache.kafka", name = "connect-file", version = kafkaVersion)

    val confluentVersion: String by project
    testImplementation(group = "io.confluent", name = "kafka-connect-avro-converter", version = confluentVersion) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }

    val avroVersion: String by project
    testImplementation(group = "org.apache.avro", name = "avro", version = avroVersion)

    val junitVersion: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.24.2")

    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "schema-registry-mock-junit5",
        version = "2.8.1"
    )
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
}

publication {
    developers {
        developer {
            name.set("Ramin Gharib")
            id.set("raminqaf")
        }
    }
}

tasks {
    compileJava {
        options.encoding = "UTF-8"
    }
    compileTestJava {
        options.encoding = "UTF-8"
    }
    test {
        useJUnitPlatform()
    }
}
