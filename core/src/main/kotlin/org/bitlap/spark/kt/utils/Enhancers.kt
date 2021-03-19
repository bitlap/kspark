package org.bitlap.spark.kt.utils

import org.bitlap.spark.kt.KSpark
import org.bitlap.spark.kt.sql.DataFrame
import org.apache.spark.sql.Row
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

fun <K, V> ScalaMap<K, V>.asJava() = JavaConverters.mapAsJavaMapConverter(this).asJava().toMutableMap()
fun <K, V> Map<K, V>.asScala(): ScalaMap<K, V> = JavaConverters.mapAsScalaMapConverter(this).asScala()
fun <K, V> Map<K, V>.asScala2(): ScalaMap2<K, V> = JavaConverters.mapAsScalaMapConverter(this).asScala()

/**
 * Row Enhance
 */
fun Row.getString(col: String) = this.getAs<String>(col)
fun Row.getBoolean(col: String) = this.getAs<Boolean>(col)
fun Row.getBytes(col: String) = this.getAs<Array<Byte>>(col)
fun <K, V> Row.getMap(col: String) = this.getAs<ScalaMap<K, V>>(col).asJava()
