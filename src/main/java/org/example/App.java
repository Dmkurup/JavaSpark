package org.example;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("JavaSparkApp")
                .getOrCreate();
        try (JavaSparkContext javaContext = new JavaSparkContext(sparkSession.sparkContext())) {
            List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
            JavaRDD<Integer> javaRDD = javaContext.parallelize(integerList, 3);
            javaRDD.foreach(new VoidFunction<Integer>() {
                @Override
                public void call(Integer integer) throws Exception {
                    System.out.println("Java RDD " + integer);
                }
            });
            Thread.sleep(1000000);
            javaContext.stop();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
