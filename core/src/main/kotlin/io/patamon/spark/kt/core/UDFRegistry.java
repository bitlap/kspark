package io.patamon.spark.kt.core;

import io.patamon.spark.kt.sql.KUserDefinedFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.*;
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

    /**
     * udf register
     */
    public void register(String name, UserDefinedFunction udf) {
        this.spark.udf().register(name, udf);
    }

    public static <R> KUserDefinedFunction udf(UDF0<R> f0, Class<R> returnType) {
        return new KUserDefinedFunction(functions.udf(f0, toDataType(returnType)));
    }
    public <R> void register(String name, UDF0<R> f0, Class<R> returnType) {
        this.spark.udf().register(name, udf(f0, returnType).get_udf());
    }

    public static <R, T1> KUserDefinedFunction udf(UDF1<T1, R> f1, Class<R> returnType) {
        return new KUserDefinedFunction(functions.udf(f1, toDataType(returnType)));
    }
    public <R, T1> void register(String name, UDF1<T1, R> f1, Class<R> returnType) {
        this.spark.udf().register(name, udf(f1, returnType).get_udf());
    }

    public static <R, T1, T2> KUserDefinedFunction udf(UDF2<T1, T2, R> f2, Class<R> returnType) {
        return new KUserDefinedFunction(functions.udf(f2, toDataType(returnType)));
    }
    public <R, T1, T2> void register(String name, UDF2<T1, T2, R> f2, Class<R> returnType) {
        this.spark.udf().register(name, udf(f2, returnType).get_udf());
    }

    public static  <R, T1, T2, T3> KUserDefinedFunction udf(UDF3<T1, T2, T3, R> f3, Class<R> returnType) {
        return new KUserDefinedFunction(functions.udf(f3, toDataType(returnType)));
    }
    public <R, T1, T2, T3> void register(String name, UDF3<T1, T2, T3, R> f3, Class<R> returnType) {
        this.spark.udf().register(name, udf(f3, returnType).get_udf());
    }

    public static <R, T1, T2, T3, T4> KUserDefinedFunction udf(UDF4<T1, T2, T3, T4, R> f4, Class<R> returnType) {
        return new KUserDefinedFunction(functions.udf(f4, toDataType(returnType)));
    }
    public <R, T1, T2, T3, T4> void register(String name, UDF4<T1, T2, T3, T4, R> f4, Class<R> returnType) {
        this.spark.udf().register(name, udf(f4, returnType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5> KUserDefinedFunction udf(UDF5<T1, T2, T3, T4, T5, R> f5, Class<R> returnType) {
        return new KUserDefinedFunction(functions.udf(f5, toDataType(returnType)));
    }
    public <R, T1, T2, T3, T4, T5> void register(String name, UDF5<T1, T2, T3, T4, T5, R> f5, Class<R> returnType) {
        this.spark.udf().register(name, udf(f5, returnType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5, T6> KUserDefinedFunction udf(UDF6<T1, T2, T3, T4, T5, T6, R> f6, Class<R> returnType) {
        return new KUserDefinedFunction(functions.udf(f6, toDataType(returnType)));
    }
    public <R, T1, T2, T3, T4, T5, T6> void register(String name, UDF6<T1, T2, T3, T4, T5, T6, R> f6, Class<R> returnType) {
        this.spark.udf().register(name, udf(f6, returnType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5, T6, T7> KUserDefinedFunction udf(UDF7<T1, T2, T3, T4, T5, T6, T7, R> f7, Class<R> returnType) {
        return new KUserDefinedFunction(functions.udf(f7, toDataType(returnType)));
    }
    public <R, T1, T2, T3, T4, T5, T6, T7> void register(String name, UDF7<T1, T2, T3, T4, T5, T6, T7, R> f7, Class<R> returnType) {
        this.spark.udf().register(name, udf(f7, returnType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5, T6, T7, T8> KUserDefinedFunction udf(UDF8<T1, T2, T3, T4, T5, T6, T7, T8, R> f8, Class<R> returnType) {
        return new KUserDefinedFunction(functions.udf(f8, toDataType(returnType)));
    }
    public <R, T1, T2, T3, T4, T5, T6, T7, T8> void register(String name, UDF8<T1, T2, T3, T4, T5, T6, T7, T8, R> f8, Class<R> returnType) {
        this.spark.udf().register(name, udf(f8, returnType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5, T6, T7, T8, T9> KUserDefinedFunction udf(UDF9<T1, T2, T3, T4, T5, T6, T7, T8, T9, R> f9, Class<R> returnType) {
        return new KUserDefinedFunction(functions.udf(f9, toDataType(returnType)));
    }
    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9> void register(String name, UDF9<T1, T2, T3, T4, T5, T6, T7, T8, T9, R> f9, Class<R> returnType) {
        this.spark.udf().register(name, udf(f9, returnType).get_udf());
    }

    public static <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> KUserDefinedFunction udf(UDF10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R> f10, Class<R> returnType) {
        return new KUserDefinedFunction(functions.udf(f10, toDataType(returnType)));
    }
    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> void register(String name, UDF10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R> f10, Class<R> returnType) {
        this.spark.udf().register(name, udf(f10, returnType).get_udf());
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> void register(String name, UDF11<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R> f11, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f11, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> void register(String name, UDF12<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, R> f12, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f12, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> void register(String name, UDF13<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, R> f13, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f13, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> void register(String name, UDF14<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, R> f14, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f14, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> void register(String name, UDF15<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, R> f15, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f15, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> void register(String name, UDF16<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, R> f16, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f16, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> void register(String name, UDF17<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, R> f17, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f17, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> void register(String name, UDF18<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, R> f18, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f18, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> void register(String name, UDF19<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, R> f19, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f19, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    public <R, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> void register(String name, UDF20<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, R> f20, Class<R> returnType) {
        UserDefinedFunction udf = functions.udf(f20, toDataType(returnType));
        this.spark.udf().register(name, udf);
    }

    private static DataType toDataType(Class<?> type) {
        return JavaTypeInference.inferDataType(type)._1;
    }

    /**
     * udaf register
     */
    public void register(String name, UserDefinedAggregateFunction udaf) {
        this.spark.udf().register(name, udaf);
    }
}
