package io.patamon.spark.kt

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

fun Dataset<Row>.df(): DataFrame = DataFrame(this)

/**
 * Desc: typealias DataFrame
 */
class DataFrame(val _ds: Dataset<Row>) {

    fun isEmpty() = _ds.isEmpty
    fun write() = _ds.write()
    fun collect() = _ds.collectAsList()

    fun createOrReplaceTempView(viewName: String) = _ds.createOrReplaceTempView(viewName)
    fun show() = _ds.show()
}

