package io.patamon.spark.kt

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.util.Utils
import scala.Symbol
import scala.Tuple2

fun Dataset<Row>.df(): DataFrame = DataFrame(this)

/**
 * Desc: typealias DataFrame
 */
class DataFrame(val _ds: Dataset<Row>) {

    // fields from dataset
    val sparkSession = _ds.sparkSession()
    val queryExecution = _ds.queryExecution()
    val sqlContext = _ds.sqlContext()

    // functions from dataset
    override fun toString() = _ds.toString()
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
    fun isStreaming() = _ds.isStreaming
    fun checkpoint(eager: Boolean = true) = _ds.checkpoint(eager)
    fun withWatermark(eventTime: String, delayThreshold: String) = _ds.withWatermark(eventTime, delayThreshold)
    fun show(numRows: Int = 20, truncate: Boolean = true) = _ds.show(numRows, truncate)
    fun show(numRows: Int = 20, truncate: Int = 20) = _ds.show(numRows, truncate)
    fun na() = _ds.na()
    fun stat() = _ds.stat()
    fun join(right: Dataset<*>) = _ds.join(right).df()
    fun join(right: Dataset<*>, usingColumn: String) = join(right, listOf(usingColumn))
    fun join(right: Dataset<*>, usingColumns: List<String>, joinType: String = "inner") =
        _ds.join(right, usingColumns.toSeq(), joinType).df()
    fun join(right: Dataset<*>, joinExprs: Column, joinType: String = "inner") =
        _ds.join(right, joinExprs, joinType).df()
    fun crossJoin(right: Dataset<*>) = _ds.crossJoin(right).df()
    fun <T> joinWith(other: Dataset<T>, condition: Column, joinType: String = "inner") = _ds.joinWith(other, condition, joinType)
    fun sortWithinPartitions(sortCol: String, vararg sortCols: String) = _ds.sortWithinPartitions(sortCol, sortCols.toSeq()).df()
    fun sortWithinPartitions(vararg sortExprs: Column) = _ds.sortWithinPartitions(sortExprs.toSeq()).df()
    fun sort(sortCol: String, vararg sortCols: String) = _ds.sort(sortCol, sortCols.toSeq()).df()
    fun sort(vararg sortExprs: Column) = _ds.sort(sortExprs.toSeq()).df()
    fun orderBy(sortCol: String, vararg sortCols: String) = _ds.orderBy(sortCol, sortCols.toSeq()).df()
    fun orderBy(vararg sortExprs: Column) = _ds.orderBy(sortExprs.toSeq()).df()
    fun apply(colName: String) = _ds.apply(colName)
    fun col(colName: String) = _ds.col(colName)
    fun `as`(alias: String) = _ds.`as`(alias).df()
    fun `as`(alias: Symbol) = _ds.`as`(alias).df()
    fun alias(alias: String) = `as`(alias)
    fun alias(alias: Symbol) = `as`(alias)
    fun select(sortCol: String, vararg sortCols: String) = _ds.select(sortCol, sortCols.toSeq()).df()
    // TODO: Column operator
    // TODO: Spark functions
    fun select(vararg sortExprs: Column) = _ds.select(sortExprs.toSeq()).df()
    fun selectExpr(vararg exprs: String) = _ds.selectExpr(exprs.toSeq()).df()
    fun filter(condition: Column) = _ds.filter(condition).df()
    fun filter(conditionExpr: String) = _ds.filter(conditionExpr).df()
    fun where(condition: Column) = _ds.where(condition).df()
    fun where(conditionExpr: String) = _ds.where(conditionExpr).df()
    fun groupBy(vararg cols: Column) = _ds.groupBy(cols.toSeq())
    fun rollup(vararg cols: Column) = _ds.rollup(cols.toSeq())
    fun cube(vararg cols: Column) = _ds.cube(cols.toSeq())
    fun agg(aggExpr: Pair<String, String>, vararg aggExprs: Pair<String, String>) =
        _ds.agg(Tuple2(aggExpr.first, aggExpr.second), aggExprs.map { Tuple2(it.first, it.second) }.toSeq()).df()
    fun agg(exprs: Map<String, String>) = _ds.agg(exprs).df()
    fun agg(expr: Column, vararg exprs: Column) = _ds.agg(expr, exprs.toSeq()).df()
    fun limit(n: Int) = _ds.limit(n).df()
    fun unionAll(other: Dataset<Row>) = _ds.unionAll(other).df()
    fun union(other: Dataset<Row>) = _ds.union(other).df()
    fun intersect(other: Dataset<Row>) = _ds.intersect(other).df()
    fun except(other: Dataset<Row>) = _ds.except(other).df()
    fun sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.random().nextLong()) =
        _ds.sample(withReplacement, fraction, seed).df()
    fun sample(fraction: Double, seed: Long = Utils.random().nextLong()) =
        sample(false, fraction, seed)
    fun randomSplit(weights: List<Double>, seed: Long = Utils.random().nextLong()) =
        _ds.randomSplitAsList(weights.toDoubleArray(), seed).map { it.df() }
    fun withColumn(colName: String, col: Column) = _ds.withColumn(colName, col).df()
    fun withColumnRenamed(existingName: String, newName: String) = _ds.withColumnRenamed(existingName, newName).df()
    fun drop(colName: String) = _ds.drop(colName).df()

    fun isEmpty() = _ds.isEmpty
    fun write() = _ds.write()
    fun collect() = _ds.collectAsList()

    fun createOrReplaceTempView(viewName: String) = _ds.createOrReplaceTempView(viewName)
    fun show() = _ds.show()
}

