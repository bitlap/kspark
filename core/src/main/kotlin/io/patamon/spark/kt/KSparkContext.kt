package io.patamon.spark.kt

import org.apache.spark.sql.SparkSession

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-01-07
 */
class KSparkContext {

    private val sparkB: SparkSession.Builder = SparkSession.builder()
    var appName = "KSpark Job"
    var master = "" // local[*]
    var hiveSupport = false
    var config = mutableMapOf<String, String>()

    fun getOrCreate(): KSpark {
        if (appName.isNotBlank()) sparkB.appName(appName)
        if (master.isNotBlank()) sparkB.master(master)
        if (config.isNotEmpty()) config.forEach { k, v -> sparkB.config(k, v) }
        if (hiveSupport) sparkB.enableHiveSupport()
        return KSpark(sparkB.orCreate)
    }
}
