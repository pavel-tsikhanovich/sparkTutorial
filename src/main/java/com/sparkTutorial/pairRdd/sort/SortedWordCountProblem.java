package com.sparkTutorial.pairRdd.sort;


import com.sparkTutorial.rdd.commons.Utils;
import java.util.Arrays;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("sortedWordCount").setMaster("local[*]");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		JavaRDD<String> lines = sparkContext.textFile("in/word_count.text");

		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordPairs = words.mapToPair(word -> new Tuple2<>(word, 1));

		JavaPairRDD<String, Integer> wordsCount = wordPairs.reduceByKey(Integer::sum);

		JavaPairRDD<Integer, String> swappedWordsCount = wordsCount.mapToPair(wordPair -> new Tuple2<>(wordPair._2(), wordPair._1()));

		JavaPairRDD<Integer, String> sortedSwappedWordsCount = swappedWordsCount.sortByKey(false);

		JavaPairRDD<String, Integer> sortedWordsCount = sortedSwappedWordsCount.mapToPair(wordPair -> new Tuple2<>(wordPair._2(), wordPair._1()));

		for (Tuple2<String, Integer> wordPair : sortedWordsCount.collect()) {
			System.out.println(wordPair._1() + " : " + wordPair._2());
		}
	}
}

