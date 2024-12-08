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
         * The Following line configures the logging level for all logs coming from
         * org.apache.
         * It suppresses logs of levels below WARN (e.g., INFO and DEBUG) to reduce log
         * verbosity.
         * Components:
         * Logger.getLogger("org.apache"): Retrieves the logger for the package
         * org.apache.
         * .setLevel(Level.WARN): Sets the logging level to WARN. The Level class comes
         * from org.apache.log4j (or similar logging framework).
         */
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        /*
         * The Following SparkConf object is used to configure the Spark application.
         * Key Methods:
         * setAppName(String name): Sets the application name, which will appear in the
         * Spark UI for monitoring. Here, the name is "Starting Spark".
         * setMaster(String master): Specifies the master URL to connect to for cluster
         * management.
         * local[*]: Indicates that Spark should run locally, using all available CPU
         * cores (threads).
         * SparkConf: This is the configuration class in Spark, used to set up the
         * application environment, including resource management and behavior.
         */
        SparkConf conf = new SparkConf()
                .setAppName("Starting Spark")
                .setMaster("local[*]");
        /*
         * Purpose: JavaSparkContext is the main entry point for interacting with a
         * Spark cluster. It provides access to various Spark features and is required
         * to create RDDs.
         * Arguments:
         * conf: The SparkConf configuration object created earlier.
         * Notes:
         * In the modern Spark API (post Spark 2.x), JavaSparkContext is often wrapped
         * inside SparkSession for ease of use.
         */
        JavaSparkContext sc = new JavaSparkContext(conf);
        /*
         * Purpose: Converts an existing collection (inputData) into a distributed
         * dataset, known as an RDD (Resilient Distributed Dataset).
         * Components:
         * inputData: A pre-existing collection of Double values. For example:
         * 
         * List<Double> inputData = Arrays.asList(1.0, 2.0, 3.0, 4.0);
         * sc.parallelize(Collection<T> data):
         * This method splits the input collection into partitions and distributes them
         * across the cluster (or threads in local mode).
         * It creates an RDD (JavaRDD<Double>), which is Spark's abstraction for a
         * distributed collection of data.
         * RDD:
         * It is immutable and distributed. Spark operates on RDDs using transformations
         * (e.g., map, filter) and actions (e.g., collect, reduce).
         */
        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        /*
         * Code Overview
         * The code computes the sum of all elements in the myRdd using the reduce
         * action and then prints the result to the console.
         * 
         * 1. The reduce Action
         * java
         * Copy code
         * Double result = myRdd.reduce((value1, value2) -> value1 + value2);
         * Purpose: The reduce method is an action in Apache Spark that aggregates all
         * elements in the RDD using a binary function. It processes the elements in
         * parallel across partitions and combines the results progressively until a
         * single output value is produced.
         * Key Components
         * Method:
         * 
         * java
         * Copy code
         * T reduce(Function2<T, T, T> func);
         * Function2<T, T, T>: Represents a binary function that takes two input values
         * of type T and produces a single output of type T.
         * In this case:
         * T = Double
         * The function (value1, value2) -> value1 + value2 sums two input Double
         * values.
         * Execution:
         * 
         * Spark applies the binary function in a tree-like manner:
         * Elements within each partition are reduced first.
         * The intermediate results from all partitions are combined to produce the
         * final result.
         * Parallelism:
         * 
         * The operation leverages Spark's distributed nature:
         * Each partition reduces its data independently.
         * Results are merged efficiently across partitions to avoid bottlenecks.
         * Flow of Execution
         * Assuming myRdd contains the values [1.0, 2.0, 3.0, 4.0] and has 2 partitions:
         * 
         * Step 1 - Partition Level Reduction:
         * Spark first computes the sums within each partition:
         * 
         * Partition 1: [1.0, 2.0] → 1.0 + 2.0 = 3.0
         * Partition 2: [3.0, 4.0] → 3.0 + 4.0 = 7.0
         * Step 2 - Combining Results:
         * Spark combines the intermediate results from all partitions:
         * 
         * 3.0 + 7.0 = 10.0
         * Step 3 - Result:
         * The final result (10.0) is returned.
         * 
         * 2. Printing the Result
         * java
         * Copy code
         * System.out.println("Reduce (Totals function) of myRdd is : " + result);
         * Purpose: Outputs the result of the reduce action to the console.
         * If the input RDD contained [1.0, 2.0, 3.0, 4.0], the output would be:
         * vbnet
         * Copy code
         * Reduce (Totals function) of myRdd is : 10.0
         * Key Notes on the reduce Method
         * Associativity:
         * The binary function used in reduce must be associative. This means:
         * 
         * (
         * 𝑎
         * +
         * 𝑏
         * )
         * +
         * 𝑐
         * =
         * 𝑎
         * +
         * (
         * 𝑏
         * +
         * 𝑐
         * )
         * (a+b)+c=a+(b+c)
         * Associativity ensures that the computation can be split and performed in
         * parallel safely.
         * Commutativity:
         * The function should ideally also be commutative (order of operations does not
         * matter):
         * 
         * 𝑎
         * +
         * 𝑏
         * =
         * 𝑏
         * +
         * 𝑎
         * a+b=b+a
         * While reduce works without commutativity, it's generally safer for parallel
         * operations to use a commutative function.
         * Non-Partitioned Operations:
         * 
         * reduce operates across all partitions, but the final aggregation happens on
         * the driver node because it returns a single value.
         * Optimization:
         * 
         * Spark's reduce is optimized to avoid unnecessary shuffles by performing
         * computations locally within partitions first.
         * Summary
         * What Happens?
         * The reduce method aggregates all elements in the RDD using the binary
         * function (value1, value2) -> value1 + value2.
         * Why Use It?
         * It efficiently computes a single aggregated result (e.g., sum, max, or any
         * custom reduction) in a distributed manner.
         * How Does It Work?
         * The binary function is applied locally within partitions, and the
         * intermediate results are progressively merged until the final result is
         * obtained.
         */
        Double result = myRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println("Reduce (Totals function) of myRdd is : " + result);
        /*
         * Purpose: Terminates the JavaSparkContext and releases the resources
         * associated with it.
         * Why is this important?:
         * It ensures no resource leaks occur.
         * In cluster mode, it disconnects from the master node.
         * Best Practice:
         * Always close the context when the job is complete.
         */
        sc.close();
        /*
         * Complete Execution Flow
         * Logging: Reduces log output for cleaner execution.
         * Configuration: Sets up a local Spark environment with a specified name and
         * resources.
         * Spark Context: Initializes the entry point for Spark operations.
         * Parallelization: Converts a local collection into a distributed RDD.
         * Shutdown: Closes the context to free resources.
         * 
         * Additional Notes
         * Spark's Functional Style: In practice, after creating myRdd, you can perform
         * operations such as transformations and actions. For example:
         * JavaRDD<Double> squaredRdd = myRdd.map(x -> x * x);
         * squaredRdd.foreach(result -> System.out.println(result));
         * 
         * Why local[*]?:
         * It is ideal for testing and debugging because Spark will run on a single
         * machine using multiple threads.
         * JavaRDD: The JavaRDD<T> class is part of Spark's Java API, enabling
         * distributed data processing.
         * 
         * Summary
         * This code initializes a basic Spark job, parallelizes a collection into an
         * RDD, and safely closes the Spark context. It is a minimal boilerplate that
         * demonstrates the essential setup required for any Spark application written
         * in Java.
         */

    }
}
