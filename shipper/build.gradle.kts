plugins {
    application
    id("com.bakdata.gradle.avro") version "2.0.1"
}

dependencies {
    implementation("org.apache.avro:avro:1.12.0")
    implementation("io.confluent:kafka-avro-serializer:7.7.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("org.awaitility:awaitility:4.2.2")
    testImplementation("junit:junit:4.13.2")
}

application {
    mainClass.set("dev.tanmay.logagg.shipper.ShipperApp")
}