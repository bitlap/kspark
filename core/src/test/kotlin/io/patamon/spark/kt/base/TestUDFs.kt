package io.patamon.spark.kt.base

import io.patamon.spark.kt.utils.ScalaMap
import io.patamon.spark.kt.utils.asJava
import java.io.Serializable

/**
 * Desc: 测试所需要的 udf
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2019-07-11
 */
object TestUDFs : Serializable {

    fun hello(input: String) = "hello $input"
    fun helloMap(map: ScalaMap<String, String>): Map<String, String> = map.asJava()
}