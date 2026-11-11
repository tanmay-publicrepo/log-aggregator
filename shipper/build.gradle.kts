plugins {
    application
}

dependencies {
    implementation(project(":model"))
    implementation("io.confluent:kafka-avro-serializer:7.7.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
}

application {
    mainClass.set("dev.tanmay.logagg.shipper.ShipperApp")
}