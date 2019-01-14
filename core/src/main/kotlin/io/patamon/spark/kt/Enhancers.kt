package io.patamon.spark.kt

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-01-07
 */
inline fun <reified T: Any> List<T>?.toDF(spark: KSpark): DataFrame = spark.createDataFrame(this ?: emptyList())
