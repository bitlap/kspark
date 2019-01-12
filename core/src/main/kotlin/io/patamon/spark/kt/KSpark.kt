package io.patamon.spark.kt

import org.apache.spark.sql.SparkSession
import java.io.Serializable
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


open class KSpark(val spark: SparkSession) : Serializable {

    val sc = spark.sparkContext()
    val sparkContext = sc
    val sqlContext = spark.sqlContext()
    val conf by lazy { spark.conf() }
    val catalog by lazy { spark.catalog() }

    fun version() = spark.version()
    fun read() = spark.read()
    fun readStream() = spark.readStream()
    fun table(tableName: String): DataFrame = spark.table(tableName)
    // fun udf() = spark.udf()

    fun createDataFrame(data: List<*>, beanClass: KClass<*>): DataFrame = spark.createDataFrame(data, beanClass.java)
    fun emptyDataFrame(): DataFrame = spark.emptyDataFrame()
    fun sql(sql: String): DataFrame = spark.sql(sql)


    fun cloneSession() = KSpark(spark.cloneSession())
    fun newSession() = KSpark(spark.newSession())


    fun stop() = spark.stop()
    fun close() = spark.close()

    fun test() {

    }
}

