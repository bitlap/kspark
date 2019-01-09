package io.patamon.spark.kt

import org.apache.spark.sql.SparkSession
import kotlin.reflect.KClass

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-01-07
 */
fun spark(context: KSparkContext.() -> Unit): KSpark =
    KSparkContext().apply(context).getOrCreate()


class KSpark(private val spark: SparkSession) {

    fun createDataFrame(data: List<*>, beanClass: KClass<*>): DataFrame = spark.createDataFrame(data, beanClass.java)

}

