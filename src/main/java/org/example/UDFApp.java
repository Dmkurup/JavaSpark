package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;

public class UDFApp {
    public static void main(String[] args) {
        UDFApp app = new UDFApp();
        app.start();
    }
    private void start(){
        SparkSession spark = SparkSession.builder().master("local").appName("UDFApp").getOrCreate();
        spark.udf().register("isOpen",new IsOpenUdf(), DataTypes.BooleanType);

        Dataset<Row> librariesDf = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .option("encoding", "cp1252")
                .load("data/Libraries.csv")
                .drop("Administrative_Authority")
                .drop("Address1")
                .drop("Address2")
                .drop("Town")
                .drop("Postcode")
                .drop("County")
                .drop("Phone")
                .drop("Email")
                .drop("Website")
                .drop("Image")
                .drop("WGS84_Latitude")
                .drop("WGS84_Longitude");
        librariesDf.show(false);
        librariesDf.printSchema();

        Dataset<Row> dateTimeDf = createDataframe(spark);
        dateTimeDf.show(false);
        dateTimeDf.printSchema();
        Dataset<Row> df = librariesDf.crossJoin(dateTimeDf);
        df.show(false);

        Dataset<Row>finalDf = df.withColumn("open",
                callUDF("isOpen",
                        col("Opening_Hours_Monday"),
                        col("Opening_Hours_Tuesday"),
                        col("Opening_Hours_Wednesday"),
                        col("Opening_Hours_Thursday"),
                        col("Opening_Hours_Friday"),
                        col("Opening_Hours_Saturday"),
                        lit("Closed"),
                        col("date")))
                .drop("Opening_Hours_Monday")
                .drop("Opening_Hours_Tuesday")
                .drop("Opening_Hours_Wednesday")
                .drop("Opening_Hours_Thursday")
                .drop("Opening_Hours_Friday")
                .drop("Opening_Hours_Saturday");
        finalDf.show(false);

    }
    private static Dataset<Row> createDataframe(SparkSession spark) {
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(
                        "date_str",
                        DataTypes.StringType,
                        false) });

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("2019-03-11 14:30:00"));
        rows.add(RowFactory.create("2019-04-27 16:00:00"));
        rows.add(RowFactory.create("2020-01-26 05:00:00"));

        return spark
                .createDataFrame(rows, schema)
                .withColumn("date", to_timestamp(col("date_str")))
                .drop("date_str");
    }
}
