plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "log-aggregator"
include("model", "shipper", "processor", "postgres-sink-connector")