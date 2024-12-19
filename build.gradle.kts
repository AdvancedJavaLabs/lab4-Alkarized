plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.apache.hadoop:hadoop-common:3.4.0")
    implementation("org.apache.hadoop:hadoop-client:3.4.0")
    testImplementation("org.apache.hadoop:hadoop-hdfs:3.4.0")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.0")
    implementation("org.apache.hadoop:hadoop-auth:3.4.0")
}

tasks.test {
    useJUnitPlatform()
}