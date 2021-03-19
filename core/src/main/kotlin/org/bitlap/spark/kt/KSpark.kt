package org.bitlap.spark.kt

import org.bitlap.spark.kt.core.KSparkContext
import org.bitlap.spark.kt.core.UDFRegistry
import org.bitlap.spark.kt.sql.DataFrame
import org.bitlap.spark.kt.utils.Types
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import java.io.Serializable
import kotlin.reflect.KClass
import org.apache.hadoop.mapreduce.InputFormat as NewInputFormat

/**
 * Global functions
 */
fun spark(context: KSparkContext.() -> Unit): KSpark = KSparkContext().apply(context).getOrCreate()
fun Dataset<Row>.df(): DataFrame = DataFrame(this)
val log = LoggerFactory.getLogger(KSpark::class.java)

/**
 * Desc: spark wrapper
 */
open class KSpark(val spark: SparkSession) : Serializable {
    val udfRegistry = UDFRegistry(spark)

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
    fun table(tableName: String): DataFrame = spark.table(tableName).df()
    fun udf() = spark.udf() // do not use, see UDFRegistry
    fun cloneSession() = KSpark(spark.cloneSession())
    fun newSession(conf: Map<String, String> = emptyMap()) = run {
        val s = spark.newSession()
        conf.forEach { s.conf().set(it.key, it.value) }
        KSpark(s)
    }
    fun stop() = spark.stop()
    fun close() = spark.close()

    // Inside Functions Create DataFrame
    inline fun <reified T: Any> createDataFrame(data: List<T>): DataFrame = spark.createDataFrame(data, T::class.java).df()
    inline fun <reified T: Any> createDataFrame(vararg data: T): DataFrame = this.createDataFrame(mutableListOf(*data))
    fun createDataFrame(data: List<*>, beanClass: KClass<*>): DataFrame = spark.createDataFrame(data, beanClass.java).df()
    fun createDataFrame(data: List<Row>, structType: StructType): DataFrame = spark.createDataFrame(data, structType).df()

    inline fun <reified T: Any> createDataFrame(data: RDD<T>): DataFrame = spark.createDataFrame(data, T::class.java).df()
    fun createDataFrame(data: RDD<*>, beanClass: KClass<*>): DataFrame = spark.createDataFrame(data, beanClass.java).df()

    inline fun <reified T: Any> createDataFrame(data: JavaRDD<T>): DataFrame = spark.createDataFrame(data, T::class.java).df()
    fun createDataFrame(data: JavaRDD<*>, beanClass: KClass<*>): DataFrame = spark.createDataFrame(data, beanClass.java).df()

    fun createDataFrame(data: RDD<Row>, structType: StructType): DataFrame = spark.createDataFrame(data, structType).df()
    fun createDataFrame(data: JavaRDD<Row>, structType: StructType): DataFrame = spark.createDataFrame(data, structType).df()
    fun createDataFrame(data: RDD<Row>, structType: StructType, needsConversion: Boolean): DataFrame
            = spark.createDataFrame(data, structType, needsConversion).df()

    fun emptyDataFrame(): DataFrame = DataFrame(spark.emptyDataFrame())
    fun sql(sql: String): DataFrame = DataFrame(spark.sql(sql))

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

    // UDF Register
    fun register(name: String, udf: UserDefinedFunction) {
        this.udfRegistry.register(name, udf)
    }
    inline fun <reified RT> register(name: String, noinline udf: () -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1> register(name: String, noinline udf: (T1) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2> register(name: String, noinline udf: (T1, T2) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3> register(name: String, noinline udf: (T1, T2, T3) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4> register(name: String, noinline udf: (T1, T2, T3, T4) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5> register(name: String, noinline udf: (T1, T2, T3, T4, T5) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    inline fun <reified RT, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> register(name: String, noinline udf: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20) -> RT) {
        this.udfRegistry.register(name, udf, Types.functionReturnTypeToDataType(udf, RT::class.java))
    }
    // UDAF Register
    fun register(name: String, udaf: UserDefinedAggregateFunction) {
        this.udfRegistry.register(name, udaf)
    }
}

