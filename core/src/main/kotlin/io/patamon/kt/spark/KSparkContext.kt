package io.patamon.kt.spark

import org.apache.spark.sql.SparkSession

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-01-07
 */
class KSparkContext {

    fun getOrCreate(): KSpark {
        val spark = SparkSession.builder()
            .appName("TEST-JOB")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "hdfs://127.0.0.1:9000/user/hive/warehouse/")
            .config("hive.metastore.uris", "thrift://localhost:9083")
            .config("spark.ui.port", "4088")
            .enableHiveSupport()
            .orCreate
        spark.sparkContext().hadoopConfiguration().set("fs.defaultFS", "hdfs://127.0.0.1:9000")
        return KSpark(spark)
    }

}
