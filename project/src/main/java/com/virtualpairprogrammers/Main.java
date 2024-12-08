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
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);
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
        JavaRDD<Integer> myRdd = sc.parallelize(inputData);
        /*
         * Reduce Overview
         * The code computes the sum of all elements in the myRdd using the reduce
         * action and then prints the result to the console.
         * 1. The reduce Action
         * 
         * Double result = myRdd.reduce((value1, value2) -> value1 + value2);
         * 
         * Purpose: The reduce method is an action in Apache Spark that aggregates all
         * elements in the RDD using a binary function. It processes the elements in
         * parallel across partitions and combines the results progressively until a
         * single output value is produced.
         * 
         * Key Components
         * Method:
         * T reduce(Function2<T, T, T> func);
         * Function2<T, T, T>: Represents a binary function that takes two input values
         * of type T and produces a single output of type T.
         * In this case:
         * T = Double
         * The function (value1, value2) -> value1 + value2 sums two input Double
         * values.
         * 
         * Execution:
         * Spark applies the binary function in a tree-like manner:
         * Elements within each partition are reduced first.
         * The intermediate results from all partitions are combined to produce the
         * final result.
         * 
         * Parallelism:
         * The operation leverages Spark's distributed nature:
         * Each partition reduces its data independently.
         * Results are merged efficiently across partitions to avoid bottlenecks.
         * 
         * Flow of Execution
         * Assuming myRdd contains the values [1.0, 2.0, 3.0, 4.0] and has 2 partitions:
         * Step 1 - Partition Level Reduction:
         * Spark first computes the sums within each partition:
         * Partition 1: [1.0, 2.0] → 1.0 + 2.0 = 3.0
         * Partition 2: [3.0, 4.0] → 3.0 + 4.0 = 7.0
         * Step 2 - Combining Results:
         * Spark combines the intermediate results from all partitions:
         * 3.0 + 7.0 = 10.0
         * Step 3 - Result:
         * The final result (10.0) is returned.
         * 
         * Key Notes on the reduce Method
         * Associativity:
         * The binary function used in reduce must be associative. This means:
         * (a+b)+c=a+(b+c)
         * Associativity ensures that the computation can be split and performed in
         * parallel safely.
         * 
         * Commutativity:
         * The function should ideally also be commutative (order of operations does not
         * matter):
         * a+b=b+a
         * While reduce works without commutativity, it's generally safer for parallel
         * operations to use a commutative function.
         * 
         * Non-Partitioned Operations:
         * reduce operates across all partitions, but the final aggregation happens on
         * the driver node because it returns a single value.
         * Optimization:
         * Spark's reduce is optimized to avoid unnecessary shuffles by performing
         * computations locally within partitions first.
         * 
         * Summary
         * What Happens?
         * The reduce method aggregates all elements in the RDD using the binary
         * function (value1, value2) -> value1 + value2.
         * 
         * Why Use It?
         * It efficiently computes a single aggregated result (e.g., sum, max, or any
         * custom reduction) in a distributed manner.
         * 
         * How Does It Work?
         * The binary function is applied locally within partitions, and the
         * intermediate results are progressively merged until the final result is
         * obtained.
         */
        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println("Reduce (Totals funcIntegertion) of myRdd is : " + result);

        /*
         * This code takes an existing RDD (myRdd), applies the square root function
         * (Math.sqrt) to each element using map (a transformation), and then prints
         * each result using foreach (an action).
         * 
         * 1. map Transformation
         * JavaRDD<Double> sqrtRdd1 = myRdd.map(value -> Math.sqrt(value));
         * Purpose
         * The map transformation applies a given function to each element in the RDD
         * and produces a new RDD containing the transformed elements.
         * Components
         * map Method:
         * JavaRDD<U> map(Function<T, U> f);
         * Input: A function f that transforms elements of type T into type U.
         * Output: A new RDD containing the transformed elements.
         * In this case:
         * T = Double (input RDD type)
         * U = Double (output RDD type)
         * Transformation function: value -> Math.sqrt(value) computes the square root
         * of each input value.
         * Execution:
         * Spark processes the map transformation in a lazy manner, meaning it does not
         * execute immediately.
         * The transformation is only triggered when an action (e.g., foreach) is
         * called.
         * Example Input and Output: If myRdd contains [1.0, 4.0, 9.0, 16.0], then:
         * The map transformation applies Math.sqrt:
         * 1.0
         * =
         * 1.0
         * 1.0
         * ​
         * =1.0
         * 4.0
         * =
         * 2.0
         * 4.0
         * ​
         * =2.0
         * 9.0
         * =
         * 3.0
         * 9.0
         * ​
         * =3.0
         * 16.0
         * =
         * 4.0
         * 16.0
         * ​
         * =4.0
         * Resulting RDD (sqrtRdd1) contains [1.0, 2.0, 3.0, 4.0].
         * 2. foreach Action
         * sqrtRdd1.foreach(value -> System.out.println(value));
         * Purpose
         * The foreach method is an action that applies a specified function to each
         * element in the RDD.
         * In this case, it prints each value to the console.
         * Components
         * foreach Method:
         * void foreach(VoidFunction<T> f);
         * Input: A function f that performs some operation on each element of type T.
         * Output: Nothing is returned (void).
         * Here, the function is value -> System.out.println(value).
         * Execution:
         * foreach triggers the execution of all preceding transformations (map in this
         * case).
         * Spark applies the function (System.out.println) to each element in the RDD in
         * parallel across the available partitions.
         * Parallel Execution:
         * Each partition executes the System.out.println function independently on its
         * data.
         * In local mode (local[*]), this parallel execution happens across available
         * CPU threads.
         * Key Points
         * Lazy Evaluation:   
         * Transformations like map are not executed immediately. Spark builds a DAG
         * (Directed Acyclic Graph) of transformations.
         * The foreach action triggers execution.
         * RDD Immutability:
         * myRdd remains unchanged. The map transformation creates a new RDD (sqrtRdd1).
         * Distributed Execution:
         * Both map and foreach operate in parallel across partitions:
         * map transforms the data within each partition.
         * foreach executes the print operation in parallel for all partitions.
         * Execution Flow
         * Assuming myRdd contains [1.0, 4.0, 9.0, 16.0], the execution flow is as
         * follows:
         * Step 1 - map Transformation:
         * Input RDD: [1.0, 4.0, 9.0, 16.0]
         * Transformation (Math.sqrt): Computes the square root of each value.
         * Resulting RDD (sqrtRdd1): [1.0, 2.0, 3.0, 4.0]
         * Step 2 - foreach Action:
         * Triggering execution.
         * Applies System.out.println to each value in sqrtRdd1.
         * Console output:
         * 1.0
         * 2.0
         * 3.0
         * 4.0
         * Final Summary
         * map: A transformation that applies Math.sqrt to each element and produces a
         * new RDD (sqrtRdd1).
         * foreach: An action that triggers execution and prints each element in the RDD
         * to the console.
         * The code demonstrates how Spark processes transformations lazily and executes
         * actions in a distributed, parallelized manner.
         */
        JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));
        sqrtRdd.foreach(value -> System.out.println(value));
        // or
        // JavaRDD<Double> sqrtRdd2 = myRdd.map(value -> Math.sqrt(value));
        // sqrtRdd2.foreach(System.out::println);

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
