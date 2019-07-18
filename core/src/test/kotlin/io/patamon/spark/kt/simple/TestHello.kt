package io.patamon.spark.kt.simple

import io.patamon.spark.kt.base.TestSparkBase
import io.patamon.spark.kt.sql.udf
import io.patamon.spark.kt.utils.getMap
import io.patamon.spark.kt.utils.getString
import org.junit.jupiter.api.Test

/**
 * Desc: Hello Spark Test
 */
object TestHello : TestSparkBase("Test Hello") {

    @Test
    fun test_create_dataFrame() {
        withTable {
            val df = spark.table("persons")
            assert(df.collect().size >= 2)
            assert(df.head().getString("name") == "p1")
        }
    }

    @Test
    fun test_udf() {
        withTable {
            val df = spark.table("persons")
            // create udf
            val udf = udf { s: String -> "hello udf $s" }
            // invoke udf
            val row = df.select(udf(df("name")) `as` "udf_name").head()
            assert(row.getString("udf_name") == "hello udf p1")
        }
    }

    @Test
    fun test_regist_udf() {
        withTable {
            withUDF {
                val df = spark.table("persons")
                // invoke udf
                val row = df.selectExpr(
                    "hello(name) as hello_name",
                    "hello_lambda(name) as hello_lambda_name"
                ).head()
                assert(row.getString("hello_name") == "hello p1")
                assert(row.getString("hello_lambda_name") == "hello lambda p1")
            }
        }
    }

    @Test
    fun test_sql() {
        withTable {
            val df = spark.sql(
                """
                select
                  id, name, age, concat_ws("_", name, age) name_age
                from persons
                """.trimIndent()
            )
            assert(df.collect().size >= 2)
            assert(df.head().getString("name_age") == "p1_22")
        }
    }

    /**
     * Support normal functions with complex types
     *
     * Not support lambda functions with complex return types like map and etc.
     *
     * @see io.patamon.spark.kt.utils.Types.functionReturnTypeToDataType
     */
    @Test
    fun test_regist_udf_map() {
        withUDF {
            val df = spark.sql(
                """
                select hello_map(map('a', '1')) map
                """.trimIndent()
            )
            val row = df.first()
            assert(row.getJavaMap<String, String>(0) == mapOf("a" to "1"))
            assert(row.getMap<String, String>("map") == mapOf("a" to "1"))
        }
    }
}
