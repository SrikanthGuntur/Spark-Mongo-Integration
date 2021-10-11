package com.dice.elux;

import jdk.dynalink.beans.StaticClass;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;




public class SparkMongoConnect {

    public static String mongoDBName = "eluxdata";
    public static String mongoCollectionName = "customer";


    public static void main(String[] args) {

        System.out.println(" **********Main Started ***********");
        System.out.println(" mongoDBName :"+mongoDBName);
        System.out.println(" mongoCollectionName :"+mongoCollectionName);
        SparkSession spark = SparkSession.builder()
                .appName("Azure-Spark-Mongo")
                .config("spark.mongodb.input.uri", "mongodb://admin:aWNhcnVzMDMzNw@172.30.131.65:27017/" + mongoDBName + "." + mongoCollectionName+ "?authSource=admin")
                .config("spark.mongodb.output.uri", "mongodb://admin:aWNhcnVzMDMzNw@172.30.131.65:27017/" + mongoDBName + "." + mongoCollectionName+ "?authSource=admin")
                .getOrCreate();
        System.out.println(" Spark Session Created");
        System.out.println("Spark Connected Version ::::" + spark.version());

        Dataset<Row> Customer = spark.read().format("json").option("multiLine",true).load("/opt/fullAccount.json");
        //Dataset<Row> Customer = spark.read().option("multiline",true).json("/opt/spark/examples/src/main/resources/employees.json");

        Customer.printSchema();
        System.out.println("Record Count ::::" +Customer.count());
        Customer.show(3);
        Customer.write().format("mongo").mode("append").save();
        System.out.println(" Mongo Insertion Completed");
        spark.close();
    }
}
