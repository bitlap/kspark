package io.patamon.spark.kt

import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

/**
 * List to DataFrame
 */
inline fun <reified T: Any> List<T>?.toDF(spark: KSpark): DataFrame = spark.createDataFrame(this ?: emptyList())

/**
 * Save functions
 */
fun DataFrame.save(
    path: String,
    format: String = "parquet",
    mode: SaveMode = SaveMode.ErrorIfExists,
    partitionBy: List<String> = emptyList(),
    options: Map<String, String> = emptyMap()
) {
    if (!this.isEmpty()) {
        this.write().format(format)
            .mode(mode)
            .partitionBy(*partitionBy.toTypedArray())
            .options(options)
            .save(path)
    }
}

fun DataFrame.saveAsParquet(
    path: String,
    mode: SaveMode = SaveMode.ErrorIfExists,
    partitionBy: List<String> = emptyList(),
    options: Map<String, String> = emptyMap()
) {
    this.save(path, "parquet", mode, partitionBy, options)
}

fun DataFrame.saveAsOrc(
    path: String,
    mode: SaveMode = SaveMode.ErrorIfExists,
    partitionBy: List<String> = emptyList(),
    options: Map<String, String> = emptyMap()
) {
    this.save(path, "orc", mode, partitionBy, options)
}


/**
 * Row Enhance
 */
fun Row.getString(col: String) = this.getAs<String>(col)
fun Row.getBoolean(col: String) = this.getAs<Boolean>(col)
