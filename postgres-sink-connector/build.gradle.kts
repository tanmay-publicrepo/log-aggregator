dependencies {
    compileOnly("org.apache.kafka:connect-api:3.9.0")
    implementation("org.postgresql:postgresql:42.7.4")
    implementation("com.zaxxer:HikariCP:5.1.0")
    testImplementation("org.apache.kafka:connect-api:3.9.0")
    testImplementation("org.assertj:assertj-core:3.27.7")
}

val providedByWorker = listOf("kafka-clients", "connect-api", "slf4j", "logback", "snappy", "zstd", "lz4")

tasks.register<Sync>("installPlugin") {
    group = "distribution"
    description = "Copy the connector jar and its third-party jars into infra/plugins/postgres-sink"

    val workerJarPrefixes = providedByWorker    // ← read it once, at configuration time

    from(tasks.jar)
    from(configurations.runtimeClasspath) {
        exclude { element -> workerJarPrefixes.any { element.name.startsWith(it) } }
    }
    into(rootProject.layout.projectDirectory.dir("infra/plugins/postgres-sink"))
}