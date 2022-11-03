package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SQLTransformation {
    public static void main(String[] args) {
        SQLTransformation app = new SQLTransformation();
        SparkSession sparkSession = SparkSession.builder().master("local").appName("DataIngestionJsonApp").getOrCreate();
        System.out.println("Using apache spark"+sparkSession.version());
        app.run(sparkSession);
    }
    private void run(SparkSession sparkSession) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("geo",DataTypes.StringType,false),
                DataTypes.createStructField("yr1980",DataTypes.DoubleType,false)
        });
        Dataset<Row> df =sparkSession.read().format("csv").schema(schema).option("header","true").load("data/population.csv");
        df.createOrReplaceTempView("geodata");
        df.printSchema();
        Dataset<Row> smallCountries =sparkSession.sql("select * from geodata where yr1980 < 1 order by yr1980 limit 5");
        smallCountries.printSchema();
        smallCountries.show(10,false);

        //creating a global view
        df.createOrReplaceGlobalTempView("geodataGlobal");
        Dataset<Row> smallCountries1 =sparkSession.sql("select * from global_temp.geodataGlobal where yr1980 < 1 order by yr1980 limit 5");
        smallCountries1.printSchema();
        smallCountries1.show(10,false);

        SparkSession sparkSession2 = sparkSession.newSession();
        Dataset<Row> largeCountries =sparkSession.sql("select * from global_temp.geodataGlobal where yr1980 > 1 order by yr1980 limit 5");
        largeCountries.printSchema();
        largeCountries.show(10,false);


    }
}
