package io.patamon.spark.kt

/**
 * List to DataFrame
 */
inline fun <reified T: Any> List<T>?.toDF(spark: KSpark): DataFrame = spark.createDataFrame(this ?: emptyList())
