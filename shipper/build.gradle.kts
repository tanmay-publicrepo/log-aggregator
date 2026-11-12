plugins {
    application
}

dependencies {
    implementation(project(":model"))
    implementation("io.confluent:kafka-avro-serializer:7.7.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("org.awaitility:awaitility:4.2.2")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.9.0")
    testImplementation("org.assertj:assertj-core:3.27.7")
    testImplementation("org.awaitility:awaitility:4.2.2")
}

application {
    mainClass.set("dev.tanmay.logagg.shipper.ShipperApp")
}