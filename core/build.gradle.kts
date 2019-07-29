val scalaVersion: String by project
val sparkVersion: String by project
val phoenixVersion: String by project

dependencies {
    compile("org.jetbrains.kotlin", "kotlin-reflect", "1.3.41")
    compile("org.apache.spark", "spark-core_$scalaVersion", sparkVersion)
    compile("org.apache.spark", "spark-sql_$scalaVersion", sparkVersion)
    compile("org.apache.spark", "spark-hive_$scalaVersion", sparkVersion)
    compile("com.databricks", "spark-csv_$scalaVersion", "1.5.0")
    compile("org.apache.phoenix", "phoenix-spark", phoenixVersion)

}
