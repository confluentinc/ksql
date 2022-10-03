plugins {
    id("io.confluent.ksql.java-conventions")
}

// generate zipfile equiv to resources.xml & maven-assembly-plugin
val packageEtc by tasks.registering(Zip::class) {
    archiveClassifier.set("resources")
    from("../config")
    include("*")
    into("${archiveBaseName.get()}-${archiveVersion.get()}")
}
