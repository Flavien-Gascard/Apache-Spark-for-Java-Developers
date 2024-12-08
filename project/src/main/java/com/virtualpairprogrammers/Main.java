package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) {
        // Test Data
        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Boilerplate Spark
        SparkConf conf = new SparkConf()
                .setAppName("Starting Spark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myRdd = sc.parallelize(inputData);


        sc.close();

    }
}
