package com.sparkTutorial.rdd.sumOfNumbers;

import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumOfNumbersProblem {

	public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */

		Logger.getLogger("org").setLevel(Level.OFF);
		SparkConf conf = new SparkConf().setAppName("sumOfNumbers").setMaster("local[1]");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		JavaRDD<String> primeNums = sparkContext.textFile("in/prime_nums.text");

		JavaRDD<Integer> numbers = primeNums.flatMap(line -> Arrays.stream(line.split("\\s+")).iterator())
			.filter(number -> !number.isEmpty())
			.map(Integer::valueOf);

		Integer sum = numbers.reduce(Integer::sum);

		System.out.println("sum of the first 100 prime number is : " + sum);
	}
}
