package io.patamon.spark.kt.utils

import io.patamon.spark.kt.log
import io.patamon.spark.kt.sql.TypeReference
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import java.lang.reflect.GenericArrayType
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.lang.reflect.WildcardType
import kotlin.reflect.KCallable
import kotlin.reflect.jvm.javaType
import kotlin.reflect.jvm.reflect

/**
 * Desc: spark [org.apache.spark.sql.types.DataType] utils
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-06-09
 */
typealias ScalaMap<K, V> = scala.collection.Map<K, V>

object Types {

    /**
     * Get DataType from [TypeReference]
     */
    fun <T> toDataType(ref: TypeReference<T>): DataType = toDataType(ref.type)

    /**
     * Get DataType from kotlin function return type
     */
    fun functionReturnTypeToDataType(func: Any, default: Class<*>): DataType {
        // normal function
        if (func is KCallable<*>) {
            return toDataType(func.returnType.javaType)
        }
        // lambda function
        if (func is Function<*>) {
            return try {
                // not support, see EmptyContainerForLocal
                toDataType(func.reflect()!!.returnType.javaType)
            } catch (e: Error) {
                // TODO: e is KotlinReflectionInternalError
                //  now return default class, maybe not correct.
                //  will fix in next kotlin-reflect version. Or handle with KType.
                log.error("Not support exception, because of [${e.message}].")
                JavaTypeInference.inferDataType(default)._1
            }
        }
        throw IllegalArgumentException("func $func is not a valid function.")
    }

    /**
     * Get DataType from [Type]
     */
    fun toDataType(type: Type): DataType = when (type) {
        is ParameterizedType -> {
            when (type.rawType as Class<*>) {
                java.util.Map::class.java,
                Map::class.java,
                scala.collection.Map::class.java -> {
                    val keyType = toDataType(type.actualTypeArguments[0])
                    val valueType = toDataType(type.actualTypeArguments[1])
                    DataTypes.createMapType(keyType, valueType)
                }
                else -> toDataType(type.actualTypeArguments[0])
            }
        }
        is GenericArrayType -> {
            toDataType(type.genericComponentType)
        }
        is WildcardType -> {
            toDataType(type.upperBounds[0])
        }
        else -> {
            JavaTypeInference.inferDataType(type as Class<*>)._1
        }
    }
}