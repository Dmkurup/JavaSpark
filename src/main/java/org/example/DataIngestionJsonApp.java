package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataIngestionJsonApp {
    public static void main(String[] args) {
        DataIngestionJsonApp app = new DataIngestionJsonApp();
        SparkSession sparkSession = SparkSession.builder().master("local").appName("DataIngestionJsonApp").getOrCreate();
        System.out.println("Using apache spark"+sparkSession.version());
        app.run(sparkSession);
    }
    private void run(SparkSession sparkSession){
        // json obj == struct json array == array in schema
        //Dataset<Row> df = sparkSession.read().format("json").load("data/jsonLines.json");
        // xml
        //Dataset<Row> df2 = sparkSession.read().format("xml")
               // .option("rowTag", "row")
                //.load("data/nasa-patents.xml");
       // Dataset<Row> df1 = sparkSession.read().text("data/romeo.txt");
       // Dataset<Row> df2 = sparkSession.read().load("data/weather.avro");
        Dataset<Row> df = sparkSession.read()
                .format("parquet")
                .load("data/alltypes_plain.parquet");
        df.show(5);
        df.printSchema();
    }
}
