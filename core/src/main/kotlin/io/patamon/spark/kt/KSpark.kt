package io.patamon.spark.kt

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import java.io.Serializable
import kotlin.reflect.KClass
import org.apache.hadoop.mapreduce.InputFormat as NewInputFormat

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

    // Inside Fields
    val sc = spark.sparkContext()
    val jsc = JavaSparkContext(sc)
    val sparkContext = sc
    val javaSparkContext = jsc
    val sqlContext = spark.sqlContext()
    val conf by lazy { spark.conf() }
    val catalog by lazy { spark.catalog() }
    val sharedState by lazy { spark.sharedState() }
    val sessionState by lazy { spark.sessionState() }

    // Inside Functions
    fun version() = spark.version()
    fun read() = spark.read()
    fun readStream() = spark.readStream()
    fun table(tableName: String): DataFrame = spark.table(tableName)
    fun udf() = spark.udf() // TODO
    fun cloneSession() = KSpark(spark.cloneSession())
    fun newSession() = KSpark(spark.newSession())
    fun stop() = spark.stop()
    fun close() = spark.close()

    // Inside Functions Create DataFrame
    inline fun <reified T: Any> createDataFrame(data: List<T>): DataFrame = spark.createDataFrame(data, T::class.java)
    inline fun <reified T: Any> createDataFrame(vararg data: T): DataFrame = createDataFrame(mutableListOf(*data))
    fun createDataFrame(data: List<*>, beanClass: KClass<*>): DataFrame = spark.createDataFrame(data, beanClass.java)
    fun createDataFrame(data: List<Row>, structType: StructType): DataFrame = spark.createDataFrame(data, structType)

    inline fun <reified T: Any> createDataFrame(data: RDD<T>): DataFrame = spark.createDataFrame(data, T::class.java)
    fun createDataFrame(data: RDD<*>, beanClass: KClass<*>): DataFrame = spark.createDataFrame(data, beanClass.java)

    inline fun <reified T: Any> createDataFrame(data: JavaRDD<T>): DataFrame = spark.createDataFrame(data, T::class.java)
    fun createDataFrame(data: JavaRDD<*>, beanClass: KClass<*>): DataFrame = spark.createDataFrame(data, beanClass.java)

    fun createDataFrame(data: RDD<Row>, structType: StructType): DataFrame = spark.createDataFrame(data, structType)
    fun createDataFrame(data: JavaRDD<Row>, structType: StructType): DataFrame = spark.createDataFrame(data, structType)
    fun createDataFrame(data: RDD<Row>, structType: StructType, needsConversion: Boolean): DataFrame = spark.createDataFrame(data, structType, needsConversion)

    fun emptyDataFrame(): DataFrame = spark.emptyDataFrame()
    fun sql(sql: String): DataFrame = spark.sql(sql)

    // Extend Functions
    fun <T> parallelize(data: List<T>, numSlices: Int = jsc.defaultParallelism()) = jsc.parallelize(data, numSlices)
    fun hadoopConfiguration() = jsc.hadoopConfiguration()
    fun <K, V, F : InputFormat<K, V>> hadoopRDD(
        conf: JobConf,
        inputFormatClass: Class<out F>,
        keyClass: Class<K>,
        valueClass: Class<V>,
        minPartitions: Int = jsc.defaultMinPartitions()
    ) = jsc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass, minPartitions)
    fun <K, V, F : InputFormat<K, V>> hadoopFile(
        path: String,
        inputFormatClass: Class<out F>,
        keyClass: Class<K>,
        valueClass: Class<V>,
        minPartitions: Int = jsc.defaultMinPartitions()
    ) = jsc.hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
    fun <T> broadcast(value: T) = jsc.broadcast(value)
    fun <K, V, F : NewInputFormat<K, V>> newAPIHadoopRDD(
        conf: Configuration,
        fClass: Class<out F>,
        kClass: Class<K>,
        vClass: Class<V>
    ) = jsc.newAPIHadoopRDD(conf, fClass, kClass, vClass)
    fun<K, V, F : NewInputFormat<K, V>> newAPIHadoopFile(
        path: String,
        fClass: Class<out F>,
        kClass: Class<K>,
        vClass: Class<V>,
        conf: Configuration
    ) = jsc.newAPIHadoopFile(path, fClass, kClass, vClass, conf)
    fun <K, V> sequenceFile(
        path: String,
        keyClass: Class<K>,
        valueClass: Class<V>,
        minPartitions: Int = jsc.defaultMinPartitions()
    ) = jsc.sequenceFile(path, keyClass, valueClass, minPartitions)
    fun <T> objectFile(path: String, minPartitions: Int = jsc.defaultMinPartitions()) = jsc.objectFile<T>(path, minPartitions)
    fun textFile(path: String, minPartitions: Int = jsc.defaultMinPartitions()) = jsc.textFile(path, minPartitions)
    fun wholeTextFiles(path: String, minPartitions: Int = jsc.defaultMinPartitions()) = jsc.wholeTextFiles(path, minPartitions)

}

