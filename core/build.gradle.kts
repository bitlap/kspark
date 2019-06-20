val sparkVersion: String by project
dependencies {
    compile("org.apache.spark", "spark-core_2.12", sparkVersion)
    compile("org.apache.spark", "spark-sql_2.12", sparkVersion)
    compile("org.apache.spark", "spark-hive_2.12", sparkVersion)
    compile("org.jetbrains.kotlin", "kotlin-reflect", "1.3.40")
}
