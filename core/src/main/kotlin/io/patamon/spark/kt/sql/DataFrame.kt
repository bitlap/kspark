package io.patamon.spark.kt.sql

import io.patamon.spark.kt.df
import io.patamon.spark.kt.utils.toSeq
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import scala.Symbol
import scala.Tuple2

/**
 * Desc: typealias DataFrame
 */
class DataFrame(val _ds: Dataset<Row>) {

    // fields from dataset
    val sparkSession = _ds.sparkSession()
    val queryExecution = _ds.queryExecution()
    val sqlContext = _ds.sqlContext()
    val rdd by lazy { _ds.rdd() }

    // functions from dataset
    override fun toString() = _ds.toString()
    operator fun get(colName: String) = _ds.apply(colName)
    fun toDF() = _ds.toDF().df()
    fun toDF(vararg colNames: String) = _ds.toDF(*colNames).df()
    // TODO: other implicit encoders
    fun <T> `as`(encoder: Encoder<T>) = _ds.`as`(encoder)
    fun schema() = _ds.schema()
    fun printSchema() = _ds.printSchema()
    fun explain(extended: Boolean = false) = _ds.explain(extended)
    fun dtypes() = _ds.dtypes().map { Pair(it._1, it._2) }
    fun columns() = _ds.columns().toList()
    fun isLocal() = _ds.isLocal
    fun isEmpty() = _ds.isEmpty
    fun isStreaming() = _ds.isStreaming
    fun checkpoint(eager: Boolean = true) = _ds.checkpoint(eager).df()
    fun localCheckpoint(eager: Boolean = true) = _ds.localCheckpoint(eager)
    fun withWatermark(eventTime: String, delayThreshold: String) = _ds.withWatermark(eventTime, delayThreshold).df()
    fun show() = _ds.show()
    fun show(numRows: Int = 20, truncate: Boolean = true) = _ds.show(numRows, truncate)
    fun show(numRows: Int = 20, truncate: Int = 20) = _ds.show(numRows, truncate)
    fun na() = _ds.na()
    fun stat() = _ds.stat()
    fun join(right: Dataset<*>) = _ds.join(right).df()
    fun join(right: Dataset<*>, usingColumn: String) = join(right, listOf(usingColumn))
    fun join(right: Dataset<*>, usingColumns: List<String>, joinType: String = "inner") =
        _ds.join(right, usingColumns.toSeq(), joinType).df()
    fun join(right: Dataset<*>, joinExprs: KColumn, joinType: String = "inner") =
        _ds.join(right, joinExprs.column, joinType).df()
    fun crossJoin(right: Dataset<*>) = _ds.crossJoin(right).df()
    fun <T> joinWith(other: Dataset<T>, condition: KColumn, joinType: String = "inner") =
        _ds.joinWith(other, condition.column, joinType)
    fun sortWithinPartitions(sortCol: String, vararg sortCols: String) =
        _ds.sortWithinPartitions(sortCol, sortCols.toSeq()).df()
    fun sortWithinPartitions(vararg sortExprs: KColumn) =
        _ds.sortWithinPartitions(sortExprs.map { it.column }.toSeq()).df()
    fun sort(sortCol: String, vararg sortCols: String) = _ds.sort(sortCol, sortCols.toSeq()).df()
    fun sort(vararg sortExprs: KColumn) =
        _ds.sort(sortExprs.map { it.column }.toSeq()).df()
    fun orderBy(sortCol: String, vararg sortCols: String) = _ds.orderBy(sortCol, sortCols.toSeq()).df()
    fun orderBy(vararg sortExprs: KColumn) = _ds.orderBy(sortExprs.map { it.column }.toSeq()).df()
    fun apply(colName: String) = _ds.apply(colName)
    fun hint(name: String, vararg parameters: Any) = _ds.hint(name, parameters)
    fun col(colName: String) = _ds.col(colName)
    fun `as`(alias: String) = _ds.`as`(alias).df()
    fun `as`(alias: Symbol) = _ds.`as`(alias).df()
    fun alias(alias: String) = `as`(alias)
    fun alias(alias: Symbol) = `as`(alias)
    fun select(sortCol: String, vararg sortCols: String) = _ds.select(sortCol, sortCols.toSeq()).df()
    // TODO: Spark functions
    fun select(vararg sortExprs: KColumn) = _ds.select(sortExprs.map { it.column }.toSeq()).df()
    fun selectExpr(vararg exprs: String) = _ds.selectExpr(exprs.toSeq()).df()
    fun filter(condition: KColumn) = _ds.filter(condition.column).df()
    fun filter(conditionExpr: String) = _ds.filter(conditionExpr).df()
    fun where(condition: KColumn) = _ds.where(condition.column).df()
    fun where(conditionExpr: String) = _ds.where(conditionExpr).df()
    fun groupBy(vararg cols: KColumn) = _ds.groupBy(cols.map { it.column }.toSeq())
    fun groupBy(col1: String, vararg cols: String) = _ds.groupBy(col1, cols.toSeq())
    fun rollup(vararg cols: KColumn) = _ds.rollup(cols.map { it.column }.toSeq())
    fun rollup(col1: String, vararg cols: String) = _ds.rollup(col1, cols.toSeq())
    fun cube(vararg cols: KColumn) = _ds.cube(cols.map { it.column }.toSeq())
    fun cube(col1: String, vararg cols: String) = _ds.cube(col1, cols.toSeq())
    fun reduce(func: (Row, Row) -> Row) = _ds.javaRDD().reduce(func)
    // TODO: groupByKey
    fun agg(aggExpr: Pair<String, String>, vararg aggExprs: Pair<String, String>) =
        _ds.agg(Tuple2(aggExpr.first, aggExpr.second), aggExprs.map { Tuple2(it.first, it.second) }.toSeq()).df()
    fun agg(exprs: Map<String, String>) = _ds.agg(exprs).df()
    fun agg(expr: KColumn, vararg exprs: KColumn) = _ds.agg(expr.column, exprs.map { it.column }.toSeq()).df()
    fun limit(n: Int) = _ds.limit(n).df()
    fun unionAll(other: Dataset<Row>) = _ds.unionAll(other).df()
    fun union(other: Dataset<Row>) = _ds.union(other).df()
    fun unionByName(other: Dataset<Row>) = _ds.unionByName(other)
    fun intersect(other: Dataset<Row>) = _ds.intersect(other).df()
    fun intersectAll(other: Dataset<Row>) = _ds.intersectAll(other).df()
    fun except(other: Dataset<Row>) = _ds.except(other).df()
    fun exceptAll(other: Dataset<Row>) = _ds.exceptAll(other).df()
    fun sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.random().nextLong()) =
        _ds.sample(withReplacement, fraction, seed).df()
    fun sample(fraction: Double, seed: Long = Utils.random().nextLong()) =
        sample(false, fraction, seed)
    fun randomSplit(weights: List<Double>, seed: Long = Utils.random().nextLong()) =
        _ds.randomSplitAsList(weights.toDoubleArray(), seed).map { it.df() }
    fun withColumn(colName: String, col: KColumn) = _ds.withColumn(colName, col.column).df()
    fun withColumnRenamed(existingName: String, newName: String) = _ds.withColumnRenamed(existingName, newName).df()
    fun drop(colName: String) = _ds.drop(colName).df()
    fun drop(vararg colNames: String) = _ds.drop(colNames.toSeq()).df()
    fun drop(col: KColumn) = _ds.drop(col.column).df()
    fun dropDuplicates() = _ds.dropDuplicates().df()
    fun dropDuplicates(col1: String, vararg cols: String) = _ds.dropDuplicates(col1, cols.toSeq()).df()
    fun dropDuplicates(colNames: List<String>) = _ds.dropDuplicates(colNames.toSeq()).df()
    fun describe(vararg cols: String) = _ds.describe(cols.toSeq()).df()
    fun summary(vararg statistics: String) = _ds.summary(statistics.toSeq()).df()
    // fun head(n: Int) = (_ds.head(n) as scala.Array<Row>).toList()
    fun head(n: Int) = _ds.takeAsList(n).toList()
    fun head() = _ds.head()
    fun first() = _ds.first()
    fun <T> transform(t: (Dataset<Row>) -> Dataset<T>) = t.invoke(_ds)
    fun filter(func: (Row) -> Boolean) = _ds.filter(func).df()
    fun <T> map(func: (Row) -> T) = _ds.javaRDD().map(func)
    fun <T> flatMap(func: (Row) -> Iterator<T>) = _ds.javaRDD().flatMap(func)
    fun <T> mapPartitions(func: (Iterator<Row>) -> Iterator<T>) = _ds.javaRDD().mapPartitions(func)
    fun foreach(func: (Row) -> Unit) = _ds.javaRDD().foreach(func)
    fun foreachPartition(func: (Iterator<Row>) -> Unit) = _ds.javaRDD().foreachPartition(func)
    fun take(n: Int) = head(n)
    fun collect() = _ds.collectAsList().toList()
    fun count() = _ds.count()
    fun repartition(numPartitions: Int) = _ds.repartition(numPartitions).df()
    fun repartition(vararg partitionExprs: KColumn) = _ds.repartition(partitionExprs.map { it.column }.toSeq()).df()
    fun repartition(numPartitions: Int, vararg partitionExprs: KColumn) =
        _ds.repartition(numPartitions, partitionExprs.map { it.column }.toSeq()).df()
    fun coalesce(numPartitions: Int) = _ds.coalesce(numPartitions).df()
    fun distinct() = _ds.distinct().df()
    fun persist() = _ds.persist().df()
    fun persist(newLevel: StorageLevel) = _ds.persist(newLevel).df()
    fun cache() = _ds.cache().df()
    fun storageLevel() = _ds.storageLevel()
    fun unpersist(blocking: Boolean = false) = _ds.unpersist(blocking)
    fun toJavaRDD() = _ds.toJavaRDD()
    fun javaRDD() = _ds.javaRDD()
    fun registerTempTable(tableName: String) = _ds.registerTempTable(tableName)
    fun createTempView(viewName: String) = _ds.createTempView(viewName)
    fun createOrReplaceTempView(viewName: String) = _ds.createOrReplaceTempView(viewName)
    fun createGlobalTempView(viewName: String) = _ds.createGlobalTempView(viewName)
    fun write() = _ds.write()
    fun writeStream() = _ds.writeStream()
    fun toJSON() = _ds.toJSON()
    fun inputFiles() = _ds.inputFiles().toList()
}
