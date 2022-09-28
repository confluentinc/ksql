/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("io.confluent.ksql.java-conventions")
}

dependencies {
    api(project(":ksqldb-engine-common"))
    api(project(":ksqldb-rest-model"))
    api(project(":ksqldb-serde"))
    api("org.apache.kafka:connect-api:7.4.0-27-ccs")
    api("org.apache.kafka:connect-runtime:7.4.0-27-ccs")
    implementation("org.codehaus.janino:janino:3.0.7")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.2")
    implementation("com.ibm.icu:icu4j:67.1")
    testImplementation(project(mapOf("path" to ":ksqldb-common", "configuration" to "testOutput")))
    testImplementation(project(mapOf("path" to ":ksqldb-engine-common", "configuration" to "testOutput")))
    testImplementation(project(":ksqldb-test-util"))
}

description = "ksqldb-execution"

tasks.withType<JavaCompile> {
    val compilerArgs = options.compilerArgs
    compilerArgs.addAll(listOf("-Xlint:all,-serial"))
    // TODO: handle compile.warnings-flag
}

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
