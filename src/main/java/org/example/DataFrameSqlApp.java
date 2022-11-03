package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions;

public class DataFrameSqlApp {
    public static void main(String[] args) {
        DataFrameSqlApp app = new DataFrameSqlApp();
        SparkSession sparkSession = SparkSession.builder().master("local").appName("DataIngestionJsonApp").getOrCreate();
        System.out.println("Using apache spark" + sparkSession.version());
        app.run(sparkSession);
    }

    private void run(SparkSession sparkSession) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("geo", DataTypes.StringType, false),
                DataTypes.createStructField("yr1980", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr1981", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr1982", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr1983", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr1984", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr1985", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr1986", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr1987", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr1988", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr1989", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr1990", DataTypes.DoubleType, false)
        });
        Dataset<Row> df = sparkSession.read().format("csv").schema(schema).option("header", "true").load("data/population.csv");
        //df.show(5);
        for(int i =1981 ;i<1990;i++){
            df=df.drop("yr"+i);
        }
        //df.show(5);


        df = df.withColumn("evolution", functions.expr("round((yr1990-yr1980)*1000000)"));
        //df.show(5);
        df.createOrReplaceTempView("geodata");
        Dataset<Row>negativeEvol = sparkSession.sql("select * from geodata where geo IS NOT NULL AND evolution<=0 order by evolution limit 25");
        negativeEvol.show(15,false);
    }
}