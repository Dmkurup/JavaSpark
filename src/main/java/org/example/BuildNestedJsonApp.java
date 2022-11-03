package org.example;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BuildNestedJsonApp {
    public static void main(String[] args) {
        BuildNestedJsonApp app = new BuildNestedJsonApp();
        app.start();
    }
    private void start(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("BuildNestedJsonApp")
                .getOrCreate();

    }
    private Column[] getColumns(Dataset<Row> df){
        String[] fieldNames = df.columns();
        Column[]columns = new Column[fieldNames.length];
        int i =0;
        for(String fieldName :fieldNames){
         columns[i++]=df.col(fieldName);
        }
        return columns;
    }
}
