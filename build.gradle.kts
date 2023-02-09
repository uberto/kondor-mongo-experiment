plugins {
    kotlin("jvm") version "1.8.0"
    application
}

group = "org.example"
version = "0.99"

repositories {
    mavenCentral()
}

val junitVersion = "5.9.1"
val junitLauncherVersion = "1.9.1"
val striktVersion = "0.34.0"

dependencies {
    api("org.mongodb:mongodb-driver-sync:4.8.2")
    api ("com.ubertob.kondor:kondor-core:1.8.0")

    implementation( "org.junit.jupiter:junit-jupiter-api:$junitVersion")

    testImplementation( "io.strikt:strikt-core:$striktVersion")

    testRuntimeOnly ("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testRuntimeOnly ("org.junit.platform:junit-platform-launcher:$junitLauncherVersion")


    testImplementation("com.ubertob.kondor:kondor-tools:1.8.0")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("MainKt")
}