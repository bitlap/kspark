package io.patamon.spark.kt.simple

import io.patamon.spark.kt.base.TestSparkBase
import io.patamon.spark.kt.sql.col
import org.junit.jupiter.api.Test
import java.io.Serializable

/**
 * Desc: Hello Spark Test
 */
object TestHello : TestSparkBase("Test Hello") {


    @Test
    fun test() {
        spark.register("f", ::hello)
        spark.createDataFrame(
            Person(1L, "mimosa", 22),
            Person(2L, "mimosa", 23)
        ).createOrReplaceTempView("test")

        spark.sql(
            """
        select *, f('udf') from test
        """.trimIndent()
        ).select(col("*"))
            .orderBy(col("age").desc())
            .show()
    }

}

fun hello(s: String): String {
    return "hello $s"
}

data class Person(val id: Long, val name: String, val age: Int) : Serializable

