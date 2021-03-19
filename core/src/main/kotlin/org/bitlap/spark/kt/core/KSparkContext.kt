package org.bitlap.spark.kt.core

import org.bitlap.spark.kt.KSpark
import org.apache.spark.sql.SparkSession
import javax.servlet.Filter

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
    var master = "" // local[*,2]
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
    var uiFilters = mutableListOf<Filter>()
    private fun uiSetUp() {
        sparkB.config("spark.ui.enabled", uiEnable)
        sparkB.config("spark.ui.port", uiPort.toLong())
        if (uiFilters.isNotEmpty())
            sparkB.config("spark.ui.filters", uiFilters.joinToString(",") { it.javaClass.canonicalName })
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
