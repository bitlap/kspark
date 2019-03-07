package io.patamon.spark.kt.sql

import io.patamon.spark.kt.utils.toSeq
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

/**
 * Desc: functions as Scala Spark [functions]
 */
fun Column.k() = KColumn(this)
fun col(colName: String) = functions.col(colName).k()
fun column(colName: String) = functions.column(colName).k()
fun lit(literal: Any) = functions.lit(literal).k()
fun when_(condition: KColumn, value: Any) = functions.`when`(condition.column, value).k()

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

