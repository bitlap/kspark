package io.patamon.spark.kt.simple

import io.patamon.spark.kt.base.TestSparkBase
import org.junit.jupiter.api.Test

/**
 * Desc: Hello Spark Test
 */
object TestHello : TestSparkBase("Test Hello") {


    @Test
    fun test() {
        spark.sql("create table if not exists test.a(a int)")
        println("aaa")
    }

}
