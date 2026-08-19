/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

plugins {
    java
    id("me.champeau.jmh") version "0.7.3"
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    mavenCentral()
    maven(url = "https://ci.opensearch.org/ci/dbc/snapshots/maven/")
    mavenLocal()
}

val jacksonVersion = "2.21.2"
val awsSdkVersion = "2.25.0"

dependencies {
    // The client under test — available to both jmh source set and main
    jmh(project(":java-client"))

    // Jackson (same versions as java-client)
    jmh("com.fasterxml.jackson.core:jackson-core:${jacksonVersion}")
    jmh("com.fasterxml.jackson.core:jackson-databind:${jacksonVersion}")
    jmh("com.fasterxml.jackson.datatype:jackson-datatype-jakarta-jsonp:${jacksonVersion}")

    // Apache HttpClient 5 transport
    jmh("org.apache.httpcomponents.client5:httpclient5:5.6.3")
    jmh("org.apache.httpcomponents.core5:httpcore5:5.4.3")
    jmh("org.apache.httpcomponents.core5:httpcore5-h2:5.4.3")

    // AWS SDK v2 transport (optional — for AOSS benchmarks)
    jmh("software.amazon.awssdk:sdk-core:${awsSdkVersion}")
    jmh("software.amazon.awssdk:auth:${awsSdkVersion}")
    jmh("software.amazon.awssdk:http-auth-aws:${awsSdkVersion}")
    jmh("software.amazon.awssdk:netty-nio-client:${awsSdkVersion}")
    jmh("software.amazon.awssdk:apache-client:${awsSdkVersion}")

    // Parsson (required Jakarta JSON implementation)
    jmh("org.eclipse.parsson:parsson:1.1.9")

    // Logging (keep JMH output clean)
    jmh("org.apache.logging.log4j:log4j-api:2.23.1")
    jmh("org.apache.logging.log4j:log4j-core:2.23.1")
}

// JMH plugin configuration
jmh {
    // JMH version
    jmhVersion = "1.37"

    // Benchmark output format: text, csv, scsv, json, latex
    resultFormat = "json"

    // Where to write results
    resultsFile = project.file("${project.buildDir}/reports/jmh/results.json")

    // Base JVM args
    jvmArgs = listOf("-server", "-Xms1g", "-Xmx1g", "-XX:+UseG1GC")

    // Forward all benchmark.* and jmh.* system properties to the forked benchmark JVM.
    // The me.champeau.jmh plugin forks a new JVM and does not inherit -D properties
    // from the Gradle JVM, so they must be forwarded explicitly.
    val forwarded = listOf(
        "benchmark.host", "benchmark.port", "benchmark.scheme",
        "benchmark.user", "benchmark.password", "benchmark.index"
    )
    jvmArgsAppend = forwarded
        .filter { System.getProperty(it) != null }
        .map { "-D$it=${System.getProperty(it)}" }

    // No warmup — state is already hot from @Setup and we're measuring write throughput
    warmupIterations = 0

    // Measurement: configurable via -Pjmh.iterations=N, default 5
    iterations = (project.findProperty("jmh.iterations") as String?)?.toInt() ?: 5
    timeOnIteration = "10s"

    // 1 JVM fork per benchmark class
    fork = 1

    // Run all benchmarks by default; override with -Pbenchmark=<pattern>
    includes = listOf(project.findProperty("benchmark")?.toString() ?: ".*")

    // Fail build on benchmark errors
    failOnError = true
}

// Convenience: run only serialization benchmarks (no live cluster needed)
tasks.register<JavaExec>("jmhSerialization") {
    dependsOn(tasks.named("jmhJar"))
    group = "benchmark"
    description = "Run only SerializationBenchmark (no OpenSearch cluster required)"
    classpath = files(tasks.named<Jar>("jmhJar").get().archiveFile)
    mainClass = "org.openjdk.jmh.Main"
    args = listOf(
        "SerializationBenchmark",
        "-wi", "2", "-i", "3", "-f", "1",
        "-rf", "json", "-rff", "${buildDir}/reports/jmh/serialization.json"
    )
}
