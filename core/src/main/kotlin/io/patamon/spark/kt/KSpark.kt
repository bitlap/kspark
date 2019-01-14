package io.patamon.spark.kt

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
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
    val jsc = JavaSparkContext(sc)
    val sparkContext = sc
    val javaSparkContext = jsc
    val sqlContext = spark.sqlContext()
    val conf by lazy { spark.conf() }
    val catalog by lazy { spark.catalog() }

    fun version() = spark.version()
    fun read() = spark.read()
    fun readStream() = spark.readStream()
    fun table(tableName: String): DataFrame = spark.table(tableName)
    // fun udf() = spark.udf()

    fun createDataFrame(data: List<*>, beanClass: KClass<*>): DataFrame = spark.createDataFrame(data, beanClass.java)
    fun createDataFrame(data: List<Row>, structType: StructType): DataFrame = spark.createDataFrame(data, structType)
    fun createDataFrame(data: RDD<*>, beanClass: KClass<*>): DataFrame = spark.createDataFrame(data, beanClass.java)
    fun createDataFrame(data: JavaRDD<*>, beanClass: KClass<*>): DataFrame = spark.createDataFrame(data, beanClass.java)
    fun createDataFrame(data: RDD<Row>, structType: StructType): DataFrame = spark.createDataFrame(data, structType)
    fun createDataFrame(data: JavaRDD<Row>, structType: StructType): DataFrame = spark.createDataFrame(data, structType)
    fun createDataFrame(data: RDD<Row>, structType: StructType, needsConversion: Boolean): DataFrame = spark.createDataFrame(data, structType, needsConversion)
    fun emptyDataFrame(): DataFrame = spark.emptyDataFrame()
    fun sql(sql: String): DataFrame = spark.sql(sql)

    fun cloneSession() = KSpark(spark.cloneSession())
    fun newSession() = KSpark(spark.newSession())


    fun stop() = spark.stop()
    fun close() = spark.close()
}

