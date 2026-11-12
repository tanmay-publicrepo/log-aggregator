plugins {
    application
}

dependencies {
    implementation(project(":model"))
    implementation("org.apache.kafka:kafka-streams:3.9.0")
    implementation("io.confluent:kafka-streams-avro-serde:7.7.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.9.0")
    testImplementation("org.assertj:assertj-core:3.27.7")
}

application {
    mainClass.set("dev.tanmay.logagg.processor.ProcessorApp")
}