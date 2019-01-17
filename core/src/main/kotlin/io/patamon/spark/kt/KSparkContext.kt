package io.patamon.spark.kt

import org.apache.spark.sql.SparkSession

/**
 * Desc: https://spark.apache.org/docs/2.4.0/configuration.html
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-01-07
 */
class KSparkContext {
    private val sparkB: SparkSession.Builder = SparkSession.builder()

    /**
     * Application Configurations
     */
    var appName = "KSpark Job"
    var master = "" // local[*]
    var hiveSupport = false
    private fun appSetUp() {
        if (appName.isNotBlank()) sparkB.appName(appName)
        if (master.isNotBlank()) sparkB.master(master)
        if (hiveSupport) sparkB.enableHiveSupport()
    }

    /**
     * Spark UI Configurations
     */
    var uiEnable = true
    var uiPort = 4040
    private fun uiSetUp() {
        sparkB.config("spark.ui.enabled", uiEnable)
        sparkB.config("spark.ui.port", uiPort.toLong())
    }

    /**
     * Other Configurations, will overwrite inside settings
     */
    var config = mutableMapOf<String, String>()
    private fun configSetUp() {
        config.forEach { k, v -> sparkB.config(k, v) }
    }

    /**
     * get spark session
     */
    fun getOrCreate(): KSpark {
        appSetUp()
        uiSetUp()
        configSetUp()
        return KSpark(sparkB.orCreate)
    }
}
