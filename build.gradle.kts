import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
	application
	kotlin("jvm") version "1.7.21"
}

group = "org.sjoblomj"
version = "1.0-SNAPSHOT"

application {
	mainClass.set("org.sjoblomj.adventofcode.MainKt")
}


repositories {
	mavenCentral()
}

dependencies {
	implementation("org.apache.kafka:kafka-clients:3.5.1")
	implementation("org.apache.kafka:kafka-streams:3.5.1")
	implementation("ch.qos.logback:logback-core:1.4.11")
	implementation("ch.qos.logback:logback-classic:1.4.11")
	implementation("net.logstash.logback:logstash-logback-encoder:7.4")
	implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.2")
	implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.2")
	testImplementation(kotlin("test"))
}

tasks {
	test {
		useJUnitPlatform()
	}

	withType<KotlinCompile> {
		kotlinOptions.jvmTarget = "1.8"
	}

	val fatJar = register<Jar>("fatJar") {
		dependsOn.addAll(listOf("compileJava", "compileKotlin", "processResources"))
		archiveClassifier.set("standalone")
		duplicatesStrategy = DuplicatesStrategy.EXCLUDE
		manifest { attributes(mapOf("Main-Class" to application.mainClass)) }
		val sourcesMain = sourceSets.main.get()
		val contents = configurations.runtimeClasspath.get()
			.map { if (it.isDirectory) it else zipTree(it) } +
			sourcesMain.output
		from(contents)
	}
	build {
		dependsOn(fatJar)
	}
	jar {
		dependsOn(fatJar)
	}
}
