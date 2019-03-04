package io.patamon.spark.kt.base

import io.patamon.spark.kt.spark
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.TestInstance

/**
 * Desc: Test spark session base
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-03-01
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class TestSparkBase(_appName: String) {
    // test spark session
    protected val spark = spark {
        appName = _appName
        master = "local[*,2]"
        uiEnable = false
        hiveSupport = true
        config = mutableMapOf(
            "spark.sql.warehouse.dir" to "file:///tmp/spark-sql-warehouse",
            // "hive.metastore.warehouse.dir" to "file:///tmp/spark-sql-warehouse",
            "spark.sql.shuffle.partitions" to "3",
            "spark.driver.host" to "127.0.0.1"
        )
    }
    // init spark session
    init {
        System.setProperty("derby.system.home", "/tmp/spark-sql-warehouse")
        // create test database
        spark.sql("create database if not exists test")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    }

    @AfterAll
    fun afterAll() {
        spark.sql("show tables in test").collect().forEach { row ->
            val table = row.getString(1)
            val temp = row.getBoolean(2)
            if (!temp) {
                spark.sql("drop table if exists test.$table")
            }
        }
    }
}
