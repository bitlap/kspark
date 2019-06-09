package io.patamon.spark.kt.core;

import io.patamon.spark.kt.sql.KUserDefinedFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.*;
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

    /**
     * udf register
     */
    public void register(String name, UserDefinedFunction udf) {
        this.spark.udf().register(name, udf);
    }

    public static <R> KUserDefinedFunction udf(UDF0<R> f0, DataType dataType) {
        return new KUserDefinedFunction(functions.udf(f0, dataType));
    }
    public <R> void register(String name, UDF0<R> f0, DataType dataType) {
        this.spark.udf().register(name, udf(f0, dataType).get_udf());
    }

    public static <R, T1> KUserDefinedFunction udf(UDF1<T1, R> f1, DataType dataType) {
        return new KUserDefinedFunction(functions.udf(f1, dataType));
    }
    public <R, T1> void register(String name, UDF1<T1, R> f1, DataType dataType) {
        this.spark.udf().register(name, udf(f1, dataType).get_udf());
    }

    public static <R, T1, T2> KUserDefinedFunction udf(UDF2<T1, T2, R> f2, DataType dataType) {
        return new KUserDefinedFunction(functions.udf(f2, dataType));
    }
    public <R, T1, T2> void register(String name, UDF2<T1, T2, R> f2, DataType dataType) {
        this.spark.udf().register(name, udf(f2, dataType).get_udf());
    }

    public static  <R, T1, T2, T3> KUserDefinedFunction udf(UDF3<T1, T2, T3, R> f3, DataType dataType) {
        return new KUserDefinedFunction(functions.udf(f3, dataType));
    }
    public <R, T1, T2, T3> void register(String name, UDF3<T1, T2, T3, R> f3, DataType dataType) {
        this.spark.udf().register(name, udf(f3, dataType).get_udf());
    }

    public static <R, T1, T2, T3, T4> KUserDefinedFunction udf(UDF4<T1, T2, T3, T4, R> f4, DataType dataType) {
        return new KUserDefinedFunction(functions.udf(f4, dataType));
    }
    public <R, T1, T2, T3, T4> void register(String name, UDF4<T1, T2, T3, T4, R> f4, DataType dataType) {
        this.spark.udf().register(name, udf(f4, dataType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5> KUserDefinedFunction udf(UDF5<T1, T2, T3, T4, T5, R> f5, DataType dataType) {
        return new KUserDefinedFunction(functions.udf(f5, dataType));
    }
    public <R, T1, T2, T3, T4, T5> void register(String name, UDF5<T1, T2, T3, T4, T5, R> f5, DataType dataType) {
        this.spark.udf().register(name, udf(f5, dataType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5, T6> KUserDefinedFunction udf(UDF6<T1, T2, T3, T4, T5, T6, R> f6, DataType dataType) {
        return new KUserDefinedFunction(functions.udf(f6, dataType));
    }
    public <R, T1, T2, T3, T4, T5, T6> void register(String name, UDF6<T1, T2, T3, T4, T5, T6, R> f6, DataType dataType) {
        this.spark.udf().register(name, udf(f6, dataType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5, T6, T7> KUserDefinedFunction udf(UDF7<T1, T2, T3, T4, T5, T6, T7, R> f7, DataType dataType) {
        return new KUserDefinedFunction(functions.udf(f7, dataType));
    }
    public <R, T1, T2, T3, T4, T5, T6, T7> void register(String name, UDF7<T1, T2, T3, T4, T5, T6, T7, R> f7, DataType dataType) {
        this.spark.udf().register(name, udf(f7, dataType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5, T6, T7, T8> KUserDefinedFunction udf(UDF8<T1, T2, T3, T4, T5, T6, T7, T8, R> f8, DataType dataType) {
        return new KUserDefinedFunction(functions.udf(f8, dataType));
    }
    public <R, T1, T2, T3, T4, T5, T6, T7, T8> void register(String name, UDF8<T1, T2, T3, T4, T5, T6, T7, T8, R> f8, DataType dataType) {
        this.spark.udf().register(name, udf(f8, dataType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5, T6, T7, T8, T9> KUserDefinedFunction udf(UDF9<T1, T2, T3, T4, T5, T6, T7, T8, T9, R> f9, DataType dataType) {
        return new KUserDefinedFunction(functions.udf(f9, dataType));
    }
    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9> void register(String name, UDF9<T1, T2, T3, T4, T5, T6, T7, T8, T9, R> f9, DataType dataType) {
        this.spark.udf().register(name, udf(f9, dataType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> KUserDefinedFunction udf(UDF10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R> f10, DataType dataType) {
        return new KUserDefinedFunction(functions.udf(f10, dataType));
    }
    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> void register(String name, UDF10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R> f10, DataType dataType) {
        this.spark.udf().register(name, udf(f10, dataType).get_udf());
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> void register(String name, UDF11<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R> f11, DataType dataType) {
        UserDefinedFunction udf = functions.udf(f11, dataType);
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> void register(String name, UDF12<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, R> f12, DataType dataType) {
        UserDefinedFunction udf = functions.udf(f12, dataType);
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> void register(String name, UDF13<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, R> f13, DataType dataType) {
        UserDefinedFunction udf = functions.udf(f13, dataType);
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> void register(String name, UDF14<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, R> f14, DataType dataType) {
        UserDefinedFunction udf = functions.udf(f14, dataType);
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> void register(String name, UDF15<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, R> f15, DataType dataType) {
        UserDefinedFunction udf = functions.udf(f15, dataType);
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> void register(String name, UDF16<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, R> f16, DataType dataType) {
        UserDefinedFunction udf = functions.udf(f16, dataType);
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> void register(String name, UDF17<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, R> f17, DataType dataType) {
        UserDefinedFunction udf = functions.udf(f17, dataType);
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> void register(String name, UDF18<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, R> f18, DataType dataType) {
        UserDefinedFunction udf = functions.udf(f18, dataType);
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> void register(String name, UDF19<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, R> f19, DataType dataType) {
        UserDefinedFunction udf = functions.udf(f19, dataType);
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> void register(String name, UDF20<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, R> f20, DataType dataType) {
        UserDefinedFunction udf = functions.udf(f20, dataType);
        this.spark.udf().register(name, udf);
    }

    /**
     * udaf register
     */
    public void register(String name, UserDefinedAggregateFunction udaf) {
        this.spark.udf().register(name, udaf);
    }
}
