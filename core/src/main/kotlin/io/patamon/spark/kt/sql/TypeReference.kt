package io.patamon.spark.kt.sql

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

/**
 * Desc: @see [com.fasterxml.jackson.core.type.TypeReference]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-06-09
 */
open class TypeReference<T> : Comparable<TypeReference<T>> {
    val type: Type =
        if (javaClass.genericSuperclass is ParameterizedType) {
            (javaClass.genericSuperclass as ParameterizedType).actualTypeArguments[0]
        } else {
            javaClass.genericSuperclass
        }

    override operator fun compareTo(other: TypeReference<T>) = 0
}
