package io.patamon.spark.kt.sql

import io.patamon.spark.kt.utils.toSeq
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
 * Desc: [UserDefinedFunction] wrapper
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-03-17
 */
open class KUserDefinedFunction(val _udf: UserDefinedFunction) {

    operator fun invoke(vararg cols: KColumn): KColumn =
        _udf.apply(cols.map { it.column }.toSeq()).k()

    operator fun invoke(cols: List<KColumn>): KColumn =
        _udf.apply(cols.map { it.column }.toSeq()).k()

    fun dataType() = _udf.dataType()
    fun nullable() = _udf.nullable()
    fun deterministic() = _udf.deterministic()
    fun withName(name: String) = KUserDefinedFunction(_udf.withName(name))
    fun asNonNullable() = KUserDefinedFunction(_udf.asNonNullable())
    fun asNondeterministic() = KUserDefinedFunction(_udf.asNondeterministic())
}