package io.patamon.spark.kt;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.api.java.UDF7;
import org.apache.spark.sql.api.java.UDF8;
import org.apache.spark.sql.api.java.UDF9;
import org.apache.spark.sql.api.java.UDF10;
import org.apache.spark.sql.catalyst.JavaTypeInference;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;

import java.io.Serializable;

/**
 * Desc: You can not use udf().register() in kotlin, because of `https://youtrack.jetbrains.com/issue/KT-14375`
 */
public class UDFRegistry implements Serializable {

    private SparkSession spark;

    public UDFRegistry(SparkSession spark) {
        this.spark = spark;
    }

    public void register(String name, UserDefinedFunction udf) {
        this.spark.udf().register(name, udf);
    }

    public <R> void register(String name, UDF0<R> f0, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f0, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1> void register(String name, UDF1<T1, R> f1, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f1, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2> void register(String name, UDF2<T1, T2, R> f2, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f2, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3> void register(String name, UDF3<T1, T2, T3, R> f3, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f3, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4> void register(String name, UDF4<T1, T2, T3, T4, R> f4, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f4, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5> void register(String name, UDF5<T1, T2, T3, T4, T5, R> f5, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f5, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6> void register(String name, UDF6<T1, T2, T3, T4, T5, T6, R> f6, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f6, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7> void register(String name, UDF7<T1, T2, T3, T4, T5, T6, T7, R> f7, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f7, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8> void register(String name, UDF8<T1, T2, T3, T4, T5, T6, T7, T8, R> f8, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f8, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9> void register(String name, UDF9<T1, T2, T3, T4, T5, T6, T7, T8, T9, R> f9, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f9, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> void register(String name, UDF10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R> f10, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f10, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    private DataType toDataType(Class<?> type) {
        return JavaTypeInference.inferDataType(type)._1;
    }

    public void register(String name, UserDefinedAggregateFunction udaf) {
        this.spark.udf().register(name, udaf);
    }
}
