package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class JsonFlattenApp {
    public static void main(String[] args) {
        JsonFlattenApp app =new JsonFlattenApp();
        app.start();
    }

    private void start(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("JsonApp")
                .getOrCreate();
        Dataset<Row> shipmentDf = spark.read().format("json")
                .option("multiline",true)
                .load("data/shipment.json");

        shipmentDf = shipmentDf.withColumn("supplier_name", shipmentDf.col("supplier.name"))
                .withColumn("supplier_city", shipmentDf.col("supplier.city"))
                .withColumn("supplier_state", shipmentDf.col("supplier.state"))
                .withColumn("supplier_country", shipmentDf.col("supplier.country"))
                .drop("supplier")
                .withColumn("customer_name", shipmentDf.col("customer.name"))
                .withColumn("customer_city", shipmentDf.col("customer.city"))
                .withColumn("customer_state", shipmentDf.col("customer.state"))
                .withColumn("customer_country", shipmentDf.col("customer.country"))
                .drop("customer")
                .withColumn("items",explode(shipmentDf.col("books")));

        shipmentDf= shipmentDf
                .withColumn("qty",shipmentDf.col("items.qty"))
                .withColumn("title",shipmentDf.col("items.title"));
        shipmentDf=shipmentDf.drop("books").drop("items");
        shipmentDf.show(5,false);
        shipmentDf.printSchema();
        shipmentDf.createOrReplaceTempView("shipmentDetail");
        Dataset<Row>bookCountDf = spark.sql("select count(*) as titleCount from shipmentDetail ");
        bookCountDf.show(false);
    }
}
