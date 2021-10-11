package com.dice.elux;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AzureSparkConnect {
    public static String mongoDBName = "eluxdata";
    public static String mongoCollectionName = "accountdata";


    public static void main(String[] args) {

        System.out.println(" **********Main Started ***********");
        System.out.println(" mongoDBName :"+mongoDBName);
        System.out.println(" mongoCollectionName :"+mongoCollectionName);
        //mongodb://admin:aWNhcnVzMDMzNw@172.30.131.65:27017/
        SparkSession spark = SparkSession.builder()
                .appName("Azure-Spark-Mongo")
                .config("spark.mongodb.input.uri", "mongodb://admin:aWNhcnVzMDMzNw@172.30.131.65:27017/" + mongoDBName + "." + mongoCollectionName+ "?authSource=admin")
                .config("spark.mongodb.output.uri", "mongodb://admin:aWNhcnVzMDMzNw@172.30.131.65:27017/" + mongoDBName + "." + mongoCollectionName+ "?authSource=admin")
                .getOrCreate();
        System.out.println(" Spark Session Created");
        System.out.println("Spark Connected Version ::::" + spark.version());

        spark.conf().set("fs.azure.sas.poc-test-container.eluxpocibmcloudrndchrszx.blob.core.windows.net",
        "?sp=racwdl&st=2021-10-04T09:06:47Z&se=2022-10-01T17:06:47Z&spr=https&sv=2020-08-04&sr=c&sig=%2BNNkYKMdDksWRZVNgrMfiVTs0VJIqNs0zIxCXtMtcBk%3D");
        System.out.println(" Spark Azure Configuration");

        //val df = spark.read.parquet("wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/<directory-name>")

        //Dataset<Row> Customer = spark.read().format("json").option("multiline",true).load("wasbs://poc-test-container@eluxpocibmcloudrndchrszx.blob.core.windows.net");
        Dataset<Row> Customer = spark.read().option("multiLine",true).json("wasbs://poc-test-container@eluxpocibmcloudrndchrszx.blob.core.windows.net/");

        //Dataset<Row> Customer = spark.read().format("json").option("multiLine",true).load("/opt/fullAccount.json");
        //Dataset<Row> Customer = spark.read().option("multiline",true).json("/opt/spark/examples/src/main/resources/employees.json");

        Customer.printSchema();
        System.out.println("Record Count ::::" +Customer.count());
        Customer.show(3);
        Customer.write().format("mongo").mode("append").save();
        System.out.println(" Mongo Insertion Completed");
        spark.close();
    }
}
