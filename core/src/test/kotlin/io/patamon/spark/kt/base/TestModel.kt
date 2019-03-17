package io.patamon.spark.kt.base

import java.io.Serializable

/**
 * Desc: some data class
 */
data class Person(val id: Long, val name: String, val age: Int) : Serializable