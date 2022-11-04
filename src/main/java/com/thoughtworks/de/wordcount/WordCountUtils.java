package com.thoughtworks.de.wordcount;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder$;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple1;
import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCountUtils {
  public static Dataset<String> splitWords(Dataset<String> ds) {
    Dataset<String> lettersOnlyDs = ds.map((MapFunction<String,String>)line -> line.replaceAll("[^a-zA-Z\\s]","").toLowerCase(),Encoders.STRING());
    Dataset<String> removeBlankLines =lettersOnlyDs.filter((FilterFunction<String>)line -> line.trim().length()>0);
    Dataset<String>justWords = removeBlankLines.flatMap((FlatMapFunction<String,String>)line->Arrays.asList(line.split(" ")).iterator(),Encoders.STRING());
    Dataset<String>removeBlankWords = justWords.filter((FilterFunction<String>) word->word.length()>0);
   // removeBlankWords.show(false);

    return removeBlankWords;//df.as(Encoders.STRING());
  }

  /*Dataset<Integer> years = file8Data.map((MapFunction<Row, Integer>) row -> row.<Integer>getAs("YEAR"), Encoders.INT());
  Dataset<Integer> newYears = years.flatMap((FlatMapFunction<Integer, Integer>) year -> {
    return Arrays.asList(year + 1, year + 2).iterator();
  }, Encoders.INT());*/

  public static Dataset<WordCountDto> countByWord( Dataset<String> ds) {
    ds.printSchema();
    Encoder<Tuple2<String, String>> encoder2 = Encoders.tuple(Encoders.STRING(), Encoders.STRING());
    Encoder<WordCountDto> encoder3 = Encoders.bean(WordCountDto.class);
    Dataset<Row> df = ds.toDF("words").groupBy("words").count().orderBy(col("count").desc());
    df=df.withColumn("countStr",df.col("count").cast(DataTypes.StringType));
    df=df.drop("count");

    List<Row> list = df.collectAsList();
    Dataset<WordCountDto> wrdDS = df.as(encoder3);
    wrdDS.show(5,false);

   Dataset<String> newDs = df.flatMap((FlatMapFunction<Row, String>) row -> Arrays.asList(row.getString(0)+","+row.getString(1)).iterator(), Encoders.STRING());

   // newDs.printSchema();
   // newDs.show(5,false);

    //Dataset<Tuple2<String, String>> res =spark.createDataset(list,encoder2);
    //Dataset<Row> df2= df.as(RowEncoder$.MODULE$.apply(df.schema()));

    df=df.withColumn("word-count",concat(df.col("words"),lit("-"),df.col("countStr")));
    df=df.drop("words").drop("countStr");
    //df.printSchema();


   // res.show();
    //Dataset<String> ds1 =
    //df=df.withColumn("word-count",concat(df.col("words"),lit("-"),df.col("count")));
    //df=df.drop("words").drop("count");
  // df.show();

    return wrdDS;
  }
}

