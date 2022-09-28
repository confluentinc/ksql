plugins {
    id("io.confluent.ksql.java-conventions")
}

// TODO: switch from transliteration to logic that depends on all subprojects
dependencies {
    implementation(project(":ksqldb-cli"))
    implementation(project(":ksqldb-common"))
    implementation(project(":ksqldb-parser"))
    implementation(project(":ksqldb-metastore"))
    implementation(project(":ksqldb-engine"))
    implementation(project(":ksqldb-tools"))
    implementation(project(":ksqldb-rest-app"))
    implementation(project(":ksqldb-serde"))
    implementation(project(":ksqldb-version-metrics-client"))
    implementation(project(":ksqldb-examples"))
    implementation(project(":ksqldb-functional-tests"))
}

// TODO: generate archives equiv to maven-assembly-plugin & 3 .xmls