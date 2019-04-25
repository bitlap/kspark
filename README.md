# KSpark

Using Apache Spark with Kotlin!

# Example

```kotlin
// create spark session
val spark = spark {
    appName = "TEST-JOB"
    master = "local[*,2]"
}

// hello: fun hello(s: String): String = "hello $s"
spark.register("func", ::hello)

// Person: data class Person(val id: Long, val name: String, val age: Int) : Serializable
spark.createDataFrame(
    Person(1L, "mimosa", 22),
    Person(2L, "poppy", 23)
).createOrReplaceTempView("test")

// query
spark.sql(
    """
    select *, func('udf') from test
    """.trimIndent()
).show()
```
