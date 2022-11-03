package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import java.util.Arrays;

public class WordCount {
    private org.apache.spark.sql.Encoder<String> Encoder;

    public static void main (String[]args){
        WordCount app = new WordCount();
        app.run();
    }
    private void run(){
        SparkSession spark = SparkSession.builder()
                             .master("local")
                             .appName("WordCount")
                             .getOrCreate();
        Dataset<Row> ds = spark.read().textFile("data/romeo.txt").toDF("words");
       // ds1.show(5);
       // Dataset<Row> ds = spark.createDataset(Arrays.asList("joey 1 for you","mojo","joey","kukki"), Encoders.STRING()).toDF("words");
        ds = ds.withColumn("word",functions.split(ds.col("words"),"\\s"));
        ds= ds.withColumn("wordSplit",functions.explode(ds.col("word")));
        ds=ds.drop("words").drop("word");
        //ds=ds.select("wordSplit").groupBy("wordSplit").count();
        //ds=ds.withColumnRenamed("wordSplit","word");
        //Dataset<Sample> ds3 = ds.as(Encoders.bean(Sample.class));
        //ds3.show(8,0,false);
        ds.write().text("file:///C:/temp/ReadMeWordCount2");
       // ds.show(8);
        //-Djava.library.path=C:/Users/Asus/winutils2/lib/native
    }
   /* val df = spark.createDataset(Seq(
            "debt ceiling", "declaration of tax", "decryption", "sweats"
    )).toDF("input")

df.select(size(split('input, "\\s+")).as("words"))
            .groupBy('words)
            .count
            .orderBy('words)
            .show*/


/*    val q = spark.
            read.
            text("README.md").
            filter(length(col("value")) > 0).
            withColumn("words", split(col("value"), "\\s+")).
            select(explode(col("words")) as "word").
            groupBy("word").
            count.
            orderBy(col("count").desc)
    scala> q.show*/
}
