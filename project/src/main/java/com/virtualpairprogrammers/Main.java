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

        /*
        The Following line configures the logging level for all logs coming from org.apache. 
        It suppresses logs of levels below WARN (e.g., INFO and DEBUG) to reduce log verbosity.
            Components:
            Logger.getLogger("org.apache"): Retrieves the logger for the package org.apache.
            .setLevel(Level.WARN): Sets the logging level to WARN. The Level class comes from org.apache.log4j (or similar logging framework).
         */
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        /*
        The Following SparkConf object is used to configure the Spark application.
        Key Methods:
        setAppName(String name): Sets the application name, which will appear in the Spark UI for monitoring. Here, the name is "Starting Spark".
        setMaster(String master): Specifies the master URL to connect to for cluster management.
        local[*]: Indicates that Spark should run locally, using all available CPU cores (threads).
        SparkConf: This is the configuration class in Spark, used to set up the application environment, including resource management and behavior.
         */
        SparkConf conf = new SparkConf()
                .setAppName("Starting Spark")
                .setMaster("local[*]");
        /*
        Purpose: JavaSparkContext is the main entry point for interacting with a Spark cluster. It provides access to various Spark features and is required to create RDDs.
        Arguments:
        conf: The SparkConf configuration object created earlier.
        Notes:
        In the modern Spark API (post Spark 2.x), JavaSparkContext is often wrapped inside SparkSession for ease of use.
         */
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
        Purpose: Converts an existing collection (inputData) into a distributed dataset, known as an RDD (Resilient Distributed Dataset).
        Components:
        inputData: A pre-existing collection of Double values. For example:

        List<Double> inputData = Arrays.asList(1.0, 2.0, 3.0, 4.0);
        sc.parallelize(Collection<T> data):
        This method splits the input collection into partitions and distributes them across the cluster (or threads in local mode).
        It creates an RDD (JavaRDD<Double>), which is Spark's abstraction for a distributed collection of data.
        RDD:
        It is immutable and distributed. Spark operates on RDDs using transformations (e.g., map, filter) and actions (e.g., collect, reduce).
         */
        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        /*
        Purpose: Terminates the JavaSparkContext and releases the resources associated with it.
        Why is this important?:
        It ensures no resource leaks occur.
        In cluster mode, it disconnects from the master node.
        Best Practice:
        Always close the context when the job is complete.
         */

        sc.close();
        /*
        Complete Execution Flow
        Logging: Reduces log output for cleaner execution.
        Configuration: Sets up a local Spark environment with a specified name and resources.
        Spark Context: Initializes the entry point for Spark operations.
        Parallelization: Converts a local collection into a distributed RDD.
        Shutdown: Closes the context to free resources.

        Additional Notes
        Spark's Functional Style: In practice, after creating myRdd, you can perform operations such as transformations and actions. For example:
        JavaRDD<Double> squaredRdd = myRdd.map(x -> x * x);
        squaredRdd.foreach(result -> System.out.println(result));

        Why local[*]?:
        It is ideal for testing and debugging because Spark will run on a single machine using multiple threads.
        JavaRDD: The JavaRDD<T> class is part of Spark's Java API, enabling distributed data processing.
        
        Summary
        This code initializes a basic Spark job, parallelizes a collection into an RDD, and safely closes the Spark context. It is a minimal boilerplate that demonstrates the essential setup required for any Spark application written in Java.
         */




    }
}
