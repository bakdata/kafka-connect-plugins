description = "A custom SMT for removing nested fields in keys and values."

plugins {
    java
    `java-library`
    id("com.github.davidmc24.gradle.plugin.avro") version "1.2.1"
    id("io.freefair.lombok") version "6.6.1"
}

group = "com.bakdata"
repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}

dependencies {
    val log4jVersion: String by project
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)

    val kafkaVersion: String by project
    compileOnly(group = "org.apache.kafka", name = "connect-transforms", version = kafkaVersion)

    testImplementation(group = "org.apache.kafka", name = "connect-runtime", version = kafkaVersion) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    testImplementation(group = "org.apache.kafka", name = "connect-api", version = kafkaVersion)
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = kafkaVersion) {
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
        version = "2.7.0"
    )
}

configure<JavaPluginExtension> {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
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
