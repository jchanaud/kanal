plugins {
    id("io.micronaut.application") version "4.5.4"
    id("com.gradleup.shadow") version "8.3.7"
    id("io.micronaut.test-resources") version "4.5.4"
    id("io.micronaut.aot") version "4.5.4"
    id("io.freefair.lombok") version "9.0.0"
}

version = "0.1"
group = "io.kanal.runner"

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
    // TODO: Remove later
    //  JDBC Connector for Development phase
    flatDir {
        dirs("libs")
    }
}

dependencies {
    annotationProcessor("io.micronaut:micronaut-http-validation")
    annotationProcessor("io.micronaut.serde:micronaut-serde-processor")
    implementation("io.micronaut.kafka:micronaut-kafka")
    implementation("io.micronaut.serde:micronaut-serde-jackson")
    implementation("io.micronaut:micronaut-jackson-databind")
    implementation("org.yaml:snakeyaml")
    implementation("com.dashjoin:jsonata:0.9.8")
    implementation("io.micronaut.micrometer:micronaut-micrometer-registry-prometheus")
    implementation("io.micronaut:micronaut-management")

    implementation("org.apache.kafka:connect-api:4.1.0")
    // I need Plugin classloading magic from connect-runtime...
    implementation("org.apache.kafka:connect-runtime:4.1.0")


    // TODO: Remove later
    //  JDBC Connector for Development phase
    implementation(fileTree(mapOf("dir" to "libs/kafka-connect-jdbc", "include" to listOf("*.jar"))))

    compileOnly("io.micronaut:micronaut-http-client")
    runtimeOnly("ch.qos.logback:logback-classic")
    testImplementation("io.micronaut:micronaut-http-client")
}


application {
    mainClass = "io.kanal.runner.Application"
}
java {
    sourceCompatibility = JavaVersion.toVersion("21")
    targetCompatibility = JavaVersion.toVersion("21")
}


graalvmNative.toolchainDetection = false

micronaut {
    runtime("netty")
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("io.kanal.runner.*")
    }
    testResources {
        sharedServer = true
    }
    aot {
        // Please review carefully the optimizations enabled below
        // Check https://micronaut-projects.github.io/micronaut-aot/latest/guide/ for more details
        optimizeServiceLoading = false
        convertYamlToJava = false
        precomputeOperations = true
        cacheEnvironment = true
        optimizeClassLoading = true
        deduceEnvironment = true
        optimizeNetty = true
        replaceLogbackXml = true
    }
}


tasks.named<io.micronaut.gradle.docker.NativeImageDockerfile>("dockerfileNative") {
    jdkVersion = "21"
}

//tasks["classes"].dependsOn(":frontend:copyClientResources")
