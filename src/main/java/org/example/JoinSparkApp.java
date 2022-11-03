package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class JoinSparkApp {
    public static void main(String[] args) {
        JoinSparkApp app = new JoinSparkApp();
        app.run();
    }

    private void run(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("JoinApp")
                .getOrCreate();
        Dataset<Row> campusDf = spark.read().format("csv")
                          .option("header","true")
                          .option("inferschema",true)
                          .load("data/InstitutionCampus.csv");
        campusDf = campusDf
                .filter("LocationType='Institution'")
                .withColumn("addressFields", split(campusDf.col("Address")," "));
        campusDf = campusDf
                .withColumn("addressFieldsCount",size(campusDf.col("addressFields")));
        campusDf = campusDf
                .withColumn ("zip9",element_at(campusDf.col("addressFields"),campusDf.col("addressFieldsCount")));
        campusDf = campusDf
                .withColumn("zip",split(campusDf.col("zip9"),"-").getItem(0))
                .withColumnRenamed("LocationName","location")
                .drop("addressFields").drop("addressFieldsCount").drop("zip9").drop("DapipId")
                .drop("OpeId").drop("ParentName").drop("ParentDapipId").drop("Address")
                .drop("GeneralPhone").drop("AdminName").drop("AdminEmail").drop("Fax").drop("UpdateDate").drop("AdminPhone").drop("LocationType");
        campusDf.show(5,false);
        campusDf.printSchema();

        Dataset<Row>censusDf = spark.read().format("csv")
                .option("header",true)
                .option("inferschema",true)
                .load("data/census.csv");
        censusDf = censusDf.drop("GEO.id")
                .drop("rescen42010")
                .drop("resbase42010")
                .drop("respop72010").drop("respop72011").drop("respop72012")
                .drop("respop72013").drop("respop72014").drop("respop72015")
                .drop("respop72016")
                .withColumnRenamed("respop72017", "pop2017")
                .withColumnRenamed("GEO.id2", "countyId")
                .withColumnRenamed("GEO.display-label", "county");
        censusDf.show(5,false);
        censusDf.printSchema();

        Dataset<Row> countyZipDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/COUNTY_ZIP_092018.csv");
        countyZipDf = countyZipDf
                .drop("res_ratio")
                .drop("bus_ratio")
                .drop("oth_ratio")
                .drop("tot_ratio");
        countyZipDf.show(5,false);

        //join campus with countyzip
       Dataset<Row>instiPerCountyDf = campusDf
               .join(countyZipDf,campusDf.col("zip").equalTo(countyZipDf.col("zip")),"inner");
        instiPerCountyDf.drop(campusDf.col("zip"));
        instiPerCountyDf.show(5,false);
        instiPerCountyDf.printSchema();

        instiPerCountyDf = instiPerCountyDf
                .join(censusDf,instiPerCountyDf.col("county").equalTo(censusDf.col("countyId")),"left");
        instiPerCountyDf=instiPerCountyDf.drop(campusDf.col("zip"))
                .drop(countyZipDf.col("county"))
                .drop("countyId")
                 .distinct();
        instiPerCountyDf.show(5,false);
    }
}
