package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class AppSample {
    public static void main(String[] args) {
        AppSample app = new AppSample();
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Sample")
                .getOrCreate();
       Dataset<Row> df = app.createDataframe(spark);
       df.show(5,false);
    }

    private static Dataset<Row> createDataframe(SparkSession spark){
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id",DataTypes.IntegerType,false),
                DataTypes.createStructField("name",DataTypes.StringType,false)
        });
        List<Row> rows = new ArrayList<Row>();
        rows.add(RowFactory.create(1,"joey"));
        rows.add(RowFactory.create(2,"joey2"));
        return spark.createDataFrame(rows,schema);
    }

}
