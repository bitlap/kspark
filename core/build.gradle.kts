val scalaVersion: String by project
val sparkVersion: String by project
val phoenixVersion: String by project

dependencies {
    implementation("org.jetbrains.kotlin", "kotlin-reflect", "1.4.31")
    implementation("org.apache.spark", "spark-core_$scalaVersion", sparkVersion)
    implementation("org.apache.spark", "spark-sql_$scalaVersion", sparkVersion)
    implementation("org.apache.spark", "spark-hive_$scalaVersion", sparkVersion)
    implementation("com.databricks", "spark-csv_$scalaVersion", "1.5.0")
    implementation("org.apache.phoenix", "phoenix-spark", phoenixVersion)

}
