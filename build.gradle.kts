import org.jetbrains.kotlin.gradle.plugin.KotlinPluginWrapper
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

allprojects {
    group = "io.patamon.kspark"
    version = "1.0-SNAPSHOT"

    apply {
        plugin<JavaPlugin>()
        plugin<KotlinPluginWrapper>()
    }

    dependencies {
        implementation(kotlin("stdlib-jdk8"))
        // testImplementation("junit", "junit", "4.12")
        testImplementation("org.junit.jupiter:junit-jupiter:5.4.0")
    }

    repositories {
        mavenLocal()
        maven("https://maven.aliyun.com/repository/public")
        mavenCentral()
    }

    tasks.withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
        kotlinOptions.suppressWarnings = true
    }

    configure<JavaPluginConvention> {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }
}

plugins {
    java
    kotlin("jvm") version "1.3.41"
}
