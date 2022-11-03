package org.example;

import org.apache.spark.sql.SparkSession;

public class DataTransformation {
   public static void main(String[]args){
       SparkSession sparkSession = SparkSession.builder().master("local").appName("IngestionApp").getOrCreate();

   }

}
