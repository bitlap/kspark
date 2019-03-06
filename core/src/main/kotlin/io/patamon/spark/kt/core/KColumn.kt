package io.patamon.spark.kt.core

import io.patamon.spark.kt.utils.toSeq
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Metadata
import scala.Symbol

/**
 * Desc: Column wrapper functions, and return Scala [Column]
 */
class KColumn(val column: Column) {

    operator fun unaryMinus() = column.`unary_$minus`()
    operator fun not() = column.`unary_$bang`()
    infix fun `===`(other: Any?) = column.`$eq$eq$eq`(other)
    infix fun `=!=`(other: Any?) = column.`$eq$bang$eq`(other)
    infix fun `!==`(other: Any?) = column.`$bang$eq$eq`(other)
    infix fun equalTo(other: Any?) = column.equalTo(other)
    infix fun notEqual(other: Any?) = column.notEqual(other)
    // infix fun `>`(other: Any?) = column.`$greater`(other)
    infix fun gt(other: Any?) = column.gt(other)
    // infix fun `<`(other: Any?) = column.`$less`(other)
    infix fun lt(other: Any?) = column.lt(other)
    // infix fun `>=`(other: Any?) = column.`$greater$eq`(other)
    infix fun geq(other: Any?) = column.geq(other)
    // infix fun `<=`(other: Any?) = column.`$less$eq`(other)
    infix fun leq(other: Any?) = column.leq(other)
    // infix fun `<=>`(other: Any?) = column.`$less$eq$greater`(other)
    infix fun eqNullSafe(other: Any?) = column.eqNullSafe(other)
    fun when_(condition: KColumn, value: Any) = column.`when`(condition.column, value)
    fun otherwise(value: Any) = column.otherwise(value)
    fun between(lowerBound: Any, upperBound: Any) = column.between(lowerBound, upperBound)
    fun isNaN() = column.isNaN
    fun isNull() = column.isNull
    fun isNotNull() = column.isNotNull
    infix fun `||`(other: KColumn) = column.`$bar$bar`(other.column)
    infix fun or(other: KColumn) = column.or(other.column)
    infix fun `&&`(other: KColumn) = column.`$amp$amp`(other.column)
    infix fun and(other: KColumn) = column.and(other.column)
    infix operator fun plus(other: Any?) = column.`$plus`(other)
    infix operator fun minus(other: Any?) = column.`$minus`(other)
    infix operator fun times(other: Any?) = column.`$times`(other)
    infix fun multiply(other: Any?) = column.multiply(other)
    infix operator fun div(other: Any?) = column.`$div`(other)
    infix fun divide(other: Any?) = column.divide(other)
    infix operator fun rem(other: Any?) = column.`$percent`(other)
    infix fun mod(other: Any?) = column.mod(other)
    infix fun isin(list: Any?) = column.isin(list)
    infix fun isInCollection(values: Iterable<Any>) = column.isInCollection(values)
    infix fun like(literal: String) = column.like(literal)
    infix fun rlike(literal: String) = column.rlike(literal)
    fun getItem(key: Any) = column.getItem(key)
    fun getField(fieldName: String) = column.getField(fieldName)
    fun substr(startPos: KColumn, len: KColumn) = column.substr(startPos.column, len.column)
    fun substr(startPos: Int, len: Int) = column.substr(startPos, len)
    fun contains(other: Any) = column.contains(other)
    fun startsWith(other: KColumn) = column.startsWith(other.column)
    fun startsWith(literal: String) = column.startsWith(literal)
    fun endsWith(other: KColumn) = column.endsWith(other.column)
    fun endsWith(literal: String) = column.endsWith(literal)
    fun alias(alias: String) = column.alias(alias)
    infix fun `as`(alias: String) = column.`as`(alias)
    fun `as`(aliases: List<String>) = column.`as`(aliases.toSeq())
    fun `as`(alias: Symbol) = column.`as`(alias)
    fun `as`(alias: String, metadata: Metadata) = column.`as`(alias, metadata)
    fun name(alias: String) = column.name(alias)
    infix fun cast(to: DataType) = column.cast(to)
    infix fun cast(to: String) = column.cast(to)
    fun desc() = column.desc()
    fun desc_nulls_first() = column.desc_nulls_first()
    fun desc_nulls_last() = column.desc_nulls_last()
    fun asc() = column.asc()
    fun asc_nulls_first() = column.asc_nulls_first()
    fun asc_nulls_last() = column.asc_nulls_last()
    fun explain(extended: Boolean) = column.explain(extended)
    fun bitwiseOR(other: Any) = column.bitwiseOR(other)
    fun bitwiseAND(other: Any) = column.bitwiseAND(other)
    fun bitwiseXOR(other: Any) = column.bitwiseXOR(other)
    fun over(window: org.apache.spark.sql.expressions.WindowSpec) = column.over(window)
    fun over() = column.over()
}

// TODO: Move to functions
fun when_(condition: Column, value: Any) = org.apache.spark.sql.functions.`when`(condition, value)
