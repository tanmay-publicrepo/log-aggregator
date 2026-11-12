subprojects {
    apply(plugin = "java")
    repositories {
        mavenCentral()
        maven("https://packages.confluent.io/maven/")
    }
    configure<JavaPluginExtension> {
        toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
    }
    dependencies {
        "implementation"("org.apache.kafka:kafka-clients:3.9.0")
        "implementation"("org.slf4j:slf4j-api:2.0.16")
        "runtimeOnly"("ch.qos.logback:logback-classic:1.5.12")
        "testImplementation"("org.junit.jupiter:junit-jupiter:5.11.3")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }
    tasks.withType<Test> { useJUnitPlatform() }
    tasks.withType<JavaCompile> { options.encoding = "UTF-8" }

    configurations.all {
    resolutionStrategy {
        force(
            "org.apache.kafka:kafka-clients:3.9.0",
            "org.apache.kafka:kafka-streams:3.9.0"
        )
    }
}
}