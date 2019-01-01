import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

allprojects {
    group = "io.patamon.kspark"
    version = "1.0-SNAPSHOT"

    dependencies {
        compile(kotlin("stdlib-jdk8"))
        compile("org.apache.spark", "spark-core_2.12", "2.4.0")
        compile("org.apache.spark", "spark-sql_2.12", "2.4.0")
        compile("org.apache.spark", "spark-hive_2.12", "2.4.0")
        testCompile("junit", "junit", "4.12")
    }

    repositories {
        mavenLocal()
        maven("https://maven.aliyun.com/repository/public")
        mavenCentral()
    }

    configure<JavaPluginConvention> {
        sourceCompatibility = JavaVersion.VERSION_1_8
    }

    tasks.withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
    }
}

plugins {
    java
    kotlin("jvm") version "1.3.11"
}
