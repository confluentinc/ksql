plugins {
    id("io.confluent.ksql.java-conventions")
}

// generate .zip file with contents matching resources.xml config
val package_console_scripts by tasks.registering(Zip::class) {
    archiveClassifier.set("resources")
    from("../bin")
    include("ksql*")
    into("${archiveBaseName.get()}-${archiveVersion.get()}")
}
