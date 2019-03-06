package io.patamon.spark.kt.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

/**
 * Desc: functions as Scala [functions]
 */
fun Column.k() = KColumn(this)
fun col(colName: String) = functions.col(colName).k()
fun column(colName: String) = functions.column(colName).k()
fun lit(literal: Any) = functions.lit(literal).k()
fun when_(condition: KColumn, value: Any) = functions.`when`(condition.column, value).k()
