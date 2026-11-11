plugins {
    `java-library`
    id("com.bakdata.gradle.avro") version "2.0.1"
}

dependencies {
    api("org.apache.avro:avro:1.12.0")
}