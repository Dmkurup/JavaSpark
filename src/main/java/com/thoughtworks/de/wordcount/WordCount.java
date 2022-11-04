package com.thoughtworks.de.wordcount;

import java.time.LocalDateTime;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class WordCount {
  static Logger log = LogManager.getRootLogger();

  /**
   * Entry point for execution.
   * @param args arguments input and output path
   */
  public static void main(String[] args) {
    log.setLevel(Level.WARN);
    log.getLogger("org.apache.spark").setLevel(Level.WARN);
    System.setProperty("hadoop.home.dir", "C:\\Users\\Asus\\winutils2\\");
    SparkSession spark = SparkSession.builder().master("local").appName("Word Count").
            getOrCreate();
    log.info("Application Initialized: " + spark.version());

    final String inputPath = (args.length > 0) ? args[0] : "./src/test/resources/data/words.txt";
    final String outputPath = (args.length > 1) ? args[1] : "./target/test-mine";
    run(spark, inputPath, outputPath);
   //"file:///C:/temp/ReadMeWordCountViaApp"
    log.info("Application Done: " + spark.sparkContext().appName());
    spark.stop();
  }

  /**
   * Run the spark application.
   * @param spark : spark session
   * @param inputPath : input path
   * @param outputPath : output path
   */
  public static void run(SparkSession spark, String inputPath, String outputPath) {
    log.info("Reading text file from: " + inputPath);
    log.info("Writing csv to directory: " + outputPath);

    Dataset<String> lines = spark.read().text(inputPath).as(Encoders.STRING());
    Dataset<String> splitWords = WordCountUtils.splitWords(lines);
    Dataset<WordCountDto> wordCounts = WordCountUtils.countByWord(splitWords);
    wordCounts.show();
    int partCount =wordCounts.rdd().partitions().length;
    System.out.println("partition count is "+partCount);
    wordCounts.coalesce(1).write().option("header",true).option("sep",",").csv(outputPath);
  }
}

