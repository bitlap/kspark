package io.patamon.spark.kt.sql

import io.patamon.spark.kt.utils.toSeq
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions

/**
 * Desc: functions as Scala Spark [functions]
 */
fun Column.k() = KColumn(this)
fun col(colName: String) = functions.col(colName).k()
fun column(colName: String) = functions.column(colName).k()
fun lit(literal: Any) = functions.lit(literal).k()

//////////////////////////////////////////////////////////////////////////////////////////////
// Sort functions
//////////////////////////////////////////////////////////////////////////////////////////////
fun asc(columnName: String) = functions.asc(columnName).k()
fun asc_nulls_first(columnName: String) = functions.asc_nulls_first(columnName).k()
fun asc_nulls_last(columnName: String) = functions.asc_nulls_last(columnName).k()
fun desc(columnName: String) = functions.desc(columnName).k()
fun desc_nulls_first(columnName: String) = functions.desc_nulls_first(columnName).k()
fun desc_nulls_last(columnName: String) = functions.desc_nulls_last(columnName).k()

//////////////////////////////////////////////////////////////////////////////////////////////
// Aggregate functions
//////////////////////////////////////////////////////////////////////////////////////////////
fun approx_count_distinct(columnName: String) = functions.approx_count_distinct(columnName).k()
fun approx_count_distinct(columnName: String, rsd: Double) = functions.approx_count_distinct(columnName, rsd).k()
fun approx_count_distinct(e: KColumn) = functions.approx_count_distinct(e.column).k()
fun approx_count_distinct(e: KColumn, rsd: Double) = functions.approx_count_distinct(e.column, rsd).k()
fun avg(columnName: String) = functions.avg(columnName).k()
fun avg(e: KColumn) = functions.avg(e.column).k()
fun collect_list(columnName: String) = functions.collect_list(columnName).k()
fun collect_list(e: KColumn) = functions.collect_list(e.column).k()
fun collect_set(columnName: String) = functions.collect_set(columnName).k()
fun collect_set(e: KColumn) = functions.collect_set(e.column).k()
fun corr(columnName1: String, columnName2: String) = functions.corr(columnName1, columnName2).k()
fun corr(column1: KColumn, column2: KColumn) = functions.corr(column1.column, column2.column).k()
fun count(columnName: String) = functions.count(columnName).k()
fun count(e: KColumn) = functions.count(e.column).k()
fun countDistinct(columnName: String, vararg columnNames: String) = functions.countDistinct(columnName, columnNames.toSeq()).k()
fun countDistinct(expr: KColumn, vararg exprs: KColumn) = functions.countDistinct(expr.column, exprs.map { it.column }.toSeq()).k()
fun covar_pop(columnName1: String, columnName2: String) = functions.covar_pop(columnName1, columnName2).k()
fun covar_pop(column1: KColumn, column2: KColumn) = functions.covar_pop(column1.column, column2.column).k()
fun covar_samp(columnName1: String, columnName2: String) = functions.covar_samp(columnName1, columnName2).k()
fun covar_samp(column1: KColumn, column2: KColumn) = functions.covar_samp(column1.column, column2.column).k()
fun first(columnName: String, ignoreNulls: Boolean = false) = functions.first(columnName, ignoreNulls).k()
fun first(e: KColumn, ignoreNulls: Boolean = false) = functions.first(e.column, ignoreNulls).k()
fun grouping(columnName: String) = functions.grouping(columnName).k()
fun grouping(e: KColumn) = functions.grouping(e.column).k()
fun grouping_id(colName: String, vararg colNames: String) = functions.grouping_id(colName, colNames.toSeq()).k()
fun grouping_id(vararg cols: KColumn) = functions.grouping_id(cols.map { it.column }.toSeq()).k()
fun kurtosis(columnName: String) = functions.kurtosis(columnName).k()
fun kurtosis(e: KColumn) = functions.kurtosis(e.column).k()
fun last(columnName: String, ignoreNulls: Boolean = false) = functions.last(columnName, ignoreNulls).k()
fun last(e: KColumn, ignoreNulls: Boolean = false) = functions.last(e.column, ignoreNulls).k()
fun max(columnName: String) = functions.max(columnName).k()
fun max(e: KColumn) = functions.max(e.column).k()
fun mean(columnName: String) = functions.mean(columnName).k()
fun min(columnName: String) = functions.min(columnName).k()
fun min(e: KColumn) = functions.min(e.column).k()
fun skewness(columnName: String) = functions.skewness(columnName)
fun skewness(e: KColumn) = functions.skewness(e.column).k()
fun stddev(columnName: String) = functions.stddev(columnName).k()
fun stddev(e: KColumn) = functions.stddev(e.column).k()
fun stddev_samp(columnName: String) = functions.stddev_samp(columnName).k()
fun stddev_samp(e: KColumn) = functions.stddev_samp(e.column).k()
fun stddev_pop(columnName: String) = functions.stddev_pop(columnName).k()
fun stddev_pop(e: KColumn) = functions.stddev_pop(e.column).k()
fun sum(columnName: String) = functions.sum(columnName).k()
fun sum(e: KColumn) = functions.sum(e.column).k()
fun sumDistinct(columnName: String) = functions.sumDistinct(columnName).k()
fun sumDistinct(e: KColumn) = functions.sumDistinct(e.column).k()
fun variance(columnName: String) = functions.variance(columnName).k()
fun variance(e: KColumn) = functions.variance(e.column).k()
fun var_samp(columnName: String) = functions.var_samp(columnName).k()
fun var_samp(e: KColumn) = functions.var_samp(e.column).k()
fun var_pop(columnName: String) = functions.var_pop(columnName).k()
fun var_pop(e: KColumn) = functions.var_pop(e.column).k()

//////////////////////////////////////////////////////////////////////////////////////////////
// Window functions
//////////////////////////////////////////////////////////////////////////////////////////////
fun unboundedPreceding() = functions.unboundedPreceding().k()
fun unboundedFollowing() = functions.unboundedFollowing().k()
fun currentRow() = functions.currentRow().k()
fun cume_dist() = functions.cume_dist().k()
fun dense_rank() = functions.dense_rank().k()
fun lag(columnName: String, offset: Int) = functions.lag(columnName, offset).k()
fun lag(e: KColumn, offset: Int) = functions.lag(e.column, offset).k()
fun lag(columnName: String, offset: Int, defaultValue: Any) = functions.lag(columnName, offset, defaultValue).k()
fun lag(e: KColumn, offset: Int, defaultValue: Any) = functions.lag(e.column, offset, defaultValue).k()
fun lead(columnName: String, offset: Int) = functions.lead(columnName, offset).k()
fun lead(e: KColumn, offset: Int) = functions.lead(e.column, offset).k()
fun lead(columnName: String, offset: Int, defaultValue: Any) = functions.lead(columnName, offset, defaultValue).k()
fun lead(e: KColumn, offset: Int, defaultValue: Any) = functions.lead(e.column, offset, defaultValue).k()
fun ntile(n: Int) = functions.ntile(n).k()
fun percent_rank() = functions.percent_rank().k()
fun rank() = functions.rank().k()
fun row_number() = functions.row_number().k()

//////////////////////////////////////////////////////////////////////////////////////////////
// Non-aggregate functions
//////////////////////////////////////////////////////////////////////////////////////////////
fun array(vararg cols: KColumn) = functions.array(cols.map { it.column }.toSeq()).k()
fun array(colName: String, vararg colNames: String) = functions.array(colName, colNames.toSeq()).k()
fun map(vararg cols: KColumn) = functions.map(cols.map { it.column }.toSeq()).k()
fun map_from_arrays(keys: KColumn, values: KColumn) = functions.map_from_arrays(keys.column, values.column).k()
fun <T> broadcast(df: Dataset<T>): Dataset<T> = functions.broadcast(df)
fun coalesce(vararg e: KColumn) = functions.coalesce(e.map { it.column }.toSeq()).k()
fun input_file_name() = functions.input_file_name().k()
fun isnan(e: KColumn) = functions.isnan(e.column).k()
fun isnull(e: KColumn) = functions.isnull(e.column).k()
fun monotonically_increasing_id() = functions.monotonically_increasing_id().k()
fun nanvl(col1: KColumn, col2: KColumn) = functions.nanvl(col1.column, col2.column).k()
fun negate(e: KColumn) = functions.negate(e.column).k()
fun not(e: KColumn) = functions.not(e.column).k()
fun rand(seed: Long) = functions.rand(seed).k()
fun rand() = functions.rand().k()
fun randn(seed: Long) = functions.randn(seed).k()
fun randn() = functions.randn().k()
fun spark_partition_id() = functions.spark_partition_id().k()
fun sqrt(e: KColumn) = functions.sqrt(e.column).k()
fun sqrt(colName: String) = functions.sqrt(colName).k()
fun struct(vararg cols: KColumn) = functions.struct(cols.map { it.column }.toSeq()).k()
fun struct(colName: String, vararg colNames: String) = functions.struct(colName, colNames.toSeq()).k()
fun when_(condition: KColumn, value: Any) = functions.`when`(condition.column, value).k()
fun bitwiseNOT(e: KColumn) = functions.bitwiseNOT(e.column).k()
fun expr(expr: String) = functions.expr(expr).k()

//////////////////////////////////////////////////////////////////////////////////////////////
// Math Functions
//////////////////////////////////////////////////////////////////////////////////////////////
fun abs(e: KColumn) = functions.abs(e.column).k()
fun abs(columnName: String) = functions.abs(col(columnName).column).k()
fun acos(e: KColumn) = functions.acos(e.column).k()
fun acos(columnName: String) = functions.acos(columnName).k()
fun asin(e: KColumn) = functions.asin(e.column).k()
fun asin(columnName: String) = functions.asin(columnName).k()
fun atan(e: KColumn) = functions.atan(e.column).k()
fun atan(columnName: String) = functions.atan(columnName).k()
fun atan2(y: KColumn, x: KColumn) = functions.atan2(y.column, x.column).k()
fun atan2(y: KColumn, xName: String) = functions.atan2(y.column, xName).k()
fun atan2(yName: String, x: KColumn) = functions.atan2(yName, x.column).k()
fun atan2(yName: String, xName: String) = functions.atan2(yName, xName).k()
fun atan2(y: KColumn, xValue: Double) = functions.atan2(y.column, xValue).k()
fun atan2(yName: String, xValue: Double) = functions.atan2(yName, xValue).k()
fun atan2(yValue: Double, x: KColumn) = functions.atan2(yValue, x.column).k()
fun atan2(yValue: Double, xName: String) = functions.atan2(yValue, xName).k()
fun bin(e: KColumn) = functions.bin(e.column).k()
fun bin(columnName: String) = functions.bin(columnName).k()
fun cbrt(e: KColumn) = functions.cbrt(e.column).k()
fun cbrt(columnName: String) = functions.cbrt(columnName).k()
fun ceil(e: KColumn) = functions.ceil(e.column).k()
fun ceil(columnName: String) = functions.ceil(columnName).k()
fun conv(num: KColumn, fromBase: Int, toBase: Int) = functions.conv(num.column, fromBase, toBase)
fun cos(e: KColumn) = functions.cos(e.column).k()
fun cos(columnName: String) = functions.cos(columnName).k()
fun cosh(e: KColumn) = functions.cosh(e.column).k()
fun cosh(columnName: String) = functions.cosh(columnName).k()
fun exp(e: KColumn) = functions.exp(e.column).k()
fun exp(columnName: String) = functions.exp(columnName).k()
fun expm1(e: KColumn) = functions.expm1(e.column).k()
fun expm1(columnName: String) = functions.expm1(columnName).k()
fun factorial(e: KColumn) = functions.factorial(e.column).k()
fun factorial(columnName: String) = functions.factorial(col(columnName).column).k()
fun floor(e: KColumn) = functions.floor(e.column).k()
fun floor(columnName: String) = functions.floor(columnName).k()
fun greatest(vararg exprs: KColumn) = functions.greatest(exprs.map { it.column }.toSeq()).k()
fun greatest(columnName: String, vararg columnNames: String) = functions.greatest(columnName, columnNames.toSeq()).k()
fun hex(column: KColumn) = functions.hex(column.column).k()
fun unhex(column: KColumn) = functions.unhex(column.column).k()
fun hypot(l: KColumn, r: KColumn) = functions.hypot(l.column, r.column).k()
fun hypot(l: KColumn, rightName: String) = functions.hypot(l.column, rightName).k()
fun hypot(leftName: String, r: KColumn) = functions.hypot(leftName, r.column).k()
fun hypot(leftName: String, rightName: String) = functions.hypot(leftName, rightName).k()
fun hypot(l: KColumn, r: Double) = functions.hypot(l.column, r).k()
fun hypot(leftName: String, r: Double) = functions.hypot(leftName, r).k()
fun hypot(l: Double, r: KColumn) = functions.hypot(l, r.column).k()
fun hypot(l: Double, rightName: String) = functions.hypot(l, rightName).k()
fun least(vararg exprs: KColumn) = functions.least(exprs.map { it.column }.toSeq()).k()
fun least(columnName: String, vararg columnNames: String) = functions.least(columnName, columnNames.toSeq()).k()
fun log(e: KColumn) = functions.log(e.column).k()
fun log(columnName: String) = functions.log(columnName).k()
fun log(base: Double, a: KColumn) = functions.log(base, a.column).k()
fun log(base: Double, columnName: String) = functions.log(base, columnName).k()
fun log10(e: KColumn) = functions.log10(e.column).k()
fun log10(columnName: String) = functions.log10(columnName).k()
fun log1p(e: KColumn) = functions.log1p(e.column).k()
fun log1p(columnName: String) = functions.log1p(columnName).k()
fun log2(e: KColumn) = functions.log(e.column).k()
fun log2(columnName: String) = functions.log(columnName).k()
fun pow(l: KColumn, r: KColumn) = functions.pow(l.column, r.column).k()
fun pow(l: KColumn, rightName: String) = functions.pow(l.column, rightName).k()
fun pow(leftName: String, r: KColumn) = functions.pow(leftName, r.column).k()
fun pow(leftName: String, rightName: String) = functions.pow(leftName, rightName).k()
fun pow(l: KColumn, r: Double) = functions.pow(l.column, r).k()
fun pow(leftName: String, r: Double) = functions.pow(leftName, r).k()
fun pow(l: Double, r: KColumn) = functions.pow(l, r.column).k()
fun pow(l: Double, rightName: String) = functions.pow(l, rightName).k()
fun pmod(dividend: KColumn, divisor: KColumn) = functions.pmod(dividend.column, divisor.column).k()
fun rint(e: KColumn) = functions.rint(e.column).k()
fun rint(columnName: String) = functions.rint(columnName).k()
fun round(e: KColumn) = functions.round(e.column).k()
fun round(e: KColumn, scale: Int) = functions.round(e.column, scale).k()
fun bround(e: KColumn) = functions.bround(e.column).k()
fun bround(e: KColumn, scale: Int) = functions.bround(e.column, scale).k()
fun shiftLeft(e: KColumn, numBits: Int) = functions.shiftLeft(e.column, numBits).k()
fun shiftRight(e: KColumn, numBits: Int) = functions.shiftRight(e.column, numBits).k()
fun shiftRightUnsigned(e: KColumn, numBits: Int) = functions.shiftRightUnsigned(e.column, numBits).k()
fun signum(e: KColumn) = functions.signum(e.column).k()
fun signum(columnName: String) = functions.signum(columnName).k()
fun sin(e: KColumn) = functions.sin(e.column).k()
fun sin(columnName: String) = functions.sin(columnName).k()
fun sinh(e: KColumn) = functions.sinh(e.column).k()
fun sinh(columnName: String) = functions.sinh(columnName).k()
fun tan(e: KColumn) = functions.tan(e.column).k()
fun tan(columnName: String) = functions.tan(columnName).k()
fun tanh(e: KColumn) = functions.tanh(e.column).k()
fun tanh(columnName: String) = functions.tanh(columnName).k()
fun degrees(e: KColumn) = functions.degrees(e.column).k()
fun degrees(columnName: String) = functions.degrees(columnName).k()
fun radians(e: KColumn) = functions.radians(e.column).k()
fun radians(columnName: String) = functions.radians(columnName).k()

//////////////////////////////////////////////////////////////////////////////////////////////
// Misc functions
//////////////////////////////////////////////////////////////////////////////////////////////
















