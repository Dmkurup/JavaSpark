package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataIngestionCsvApp {

    public static void main(String[] args) {
        DataIngestionCsvApp app = new DataIngestionCsvApp();
        SparkSession sparkSession = SparkSession.builder().master("local").appName("DataIngestionCsvApp").getOrCreate();
        System.out.println("Using apache spark"+sparkSession.version());
        app.run(sparkSession);
    }

    private void run(SparkSession sparkSession){
       Dataset<Row> df =sparkSession.read().format("csv")
               .option("header","true")
               .option("multiline","true")
               .option("sep",";")
               .option("quote","*")
               .option("dateFormat", "MM/dd/yyyy")
               .option("inferSchema",true)
               .load("data/books.csv");
        df.show(7,90);
        df.printSchema();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id",DataTypes.IntegerType,false),
                DataTypes.createStructField("authordId",DataTypes.IntegerType,false),
                DataTypes.createStructField("bookTitle",DataTypes.StringType,false),
                DataTypes.createStructField("releaseDate",DataTypes.DateType,true),
                DataTypes.createStructField("url",DataTypes.StringType,false)
        });
        Dataset<Row> df1 = sparkSession. read().format("csv")
                .option("header", "true")
                .option("multiline","true")
                .option("sep",";")
                .option("quote","*")
                .option("dateFormat", "MM/dd/yyyy")
                .schema(schema)
                .load("data/books.csv");
        df1.show(7,90);
        df1.printSchema();
    }

}
