package io.patamon.spark.kt.utils

import io.patamon.spark.kt.KSpark
import io.patamon.spark.kt.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import scala.collection.JavaConverters


/**
 * print string
 */
fun Any?.printString() {
    println(this.toString())
}

/**
 * List to DataFrame
 */
inline fun <reified T: Any> List<T>?.toDF(spark: KSpark): DataFrame = spark.createDataFrame(this ?: emptyList())


/**
 * Array/List to scala seq
 */
fun <T> List<T>?.toSeq() =
    JavaConverters.asScalaIteratorConverter((this ?: emptyList()).iterator()).asScala().toSeq()
inline fun <reified T: Any> Array<out T>?.toSeq() =
    (this ?: emptyArray()).toList().toSeq()

/**
 * Scala array to list
 */
inline fun <reified T: Any> scala.Array<T>?.toList(): List<T> {
    this ?: return emptyList()
    val bb = object : Iterator<T> {
        private var i = 0
        override fun next() = this@toList.apply(i++)
        override fun hasNext() = i < this@toList.length()
    }
    return bb.asSequence().toList()
}

fun <K, V> ScalaMap<K, V>.asJava() = JavaConverters.mapAsJavaMap(this).toMutableMap()
fun <K, V> Map<K, V>.asScala() = JavaConverters.mapAsScalaMap(this)

/**
 * Row Enhance
 */
fun Row.getString(col: String) = this.getAs<String>(col)
fun Row.getBoolean(col: String) = this.getAs<Boolean>(col)
fun Row.getBytes(col: String) = this.getAs<Array<Byte>>(col)

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
            .partitionBy(partitionBy.toTypedArray().toSeq())
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
