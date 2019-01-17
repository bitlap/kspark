import io.patamon.spark.kt.spark
import java.io.Serializable

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-01-02
 */
fun main(args: Array<String>) {
    val spark = spark {
        appName = "TEST-JOB"
        master = "local[*]"
        hiveSupport = true
        config = mutableMapOf(
            "spark.sql.warehouse.dir" to "hdfs://127.0.0.1:9000/user/hive/warehouse/",
            "hive.metastore.uris" to "thrift://localhost:9083",
            "spark.ui.port" to "4088"
        )
    }

    spark.register("f", ::hello)

    spark.createDataFrame(
        Person(1L, "mimosa", 22),
        Person(2L, "mimosa", 23)
    ).createOrReplaceTempView("test")

    spark.sql(
        """
        select *, f('udf') from test
        """.trimIndent()
    ).show()
}

fun hello(s: String): String {
    return "hello $s"
}

data class Person(val id: Long, val name: String, val age: Int) : Serializable
