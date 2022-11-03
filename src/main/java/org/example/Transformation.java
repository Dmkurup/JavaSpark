package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.expr;

public class Transformation {
    public static void main(String[] args) {
        Transformation app = new Transformation();
        app.run();
    }
    private void run(){
        SparkSession spark = SparkSession.builder()
            .master("local")
            .appName("TransformApp")
            .getOrCreate();
        Dataset<Row> interMediateDF = spark
            .read()
            .format("csv")
            .option("header","true")
            .option("inferSchema",true)
            .load("data/census.csv");
        interMediateDF =interMediateDF
            .drop("GEO.id")
            .withColumnRenamed("GEO.id2","id")
            .withColumnRenamed("GEO.display-label","label")
            .withColumnRenamed("rescen42010","real2010")
            .drop("resbase42010")
            .withColumnRenamed("respop72010","est2010")
            .withColumnRenamed("respop72011","est2011")
            .withColumnRenamed("respop72012","est2012")
            .withColumnRenamed("respop72013","est2013")
            .withColumnRenamed("respop72014","est2014")
            .withColumnRenamed("respop72015","est2015")
            .withColumnRenamed("respop72016","est2016")
            .withColumnRenamed("respop72017","est2017");

        interMediateDF = interMediateDF.withColumn("countyState"
                ,split(interMediateDF.col("label"),","));
        interMediateDF = interMediateDF
                .withColumn("stateId",expr("int(id/1000)"))
                .withColumn("countyId",expr("(id%1000)"));
        interMediateDF = interMediateDF
                .withColumn("county",interMediateDF.col("countyState").getItem(0))
                .withColumn("state",interMediateDF.col("countyState").getItem(1))
                .drop("countyState")
                .drop("label");
       Dataset<Row>statDf =  interMediateDF
                .withColumn("diff",expr("est2010-real2010"))
                .withColumn("growth",expr("est2017-est2010"))
                .drop("id")
                .drop("real2010")
                .drop("est2010").drop("est2011").drop("est2012").drop("est2013")
                .drop("est2014").drop("est2015").drop("est2016").drop("est2017");

        statDf = statDf.sort(statDf.col("growth"));

        statDf.printSchema();
        statDf.show(5,true);

/*        StructType schema = DataTypes.createStructType(new StructType[]{
                DataTypes.createStructField()
        })*/
        Dataset<Row> df = spark.read().format("csv")
                .option("header","true")
                //.schema(schema)
                .load("data/census.csv");
    }
}
