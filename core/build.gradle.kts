val spark_version: String by project
dependencies {
    compile("org.apache.spark", "spark-core_2.12", spark_version)
    compile("org.apache.spark", "spark-sql_2.12", spark_version)
    compile("org.apache.spark", "spark-hive_2.12", spark_version)
}
