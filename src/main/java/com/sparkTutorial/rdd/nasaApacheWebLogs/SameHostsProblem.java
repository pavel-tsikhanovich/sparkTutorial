package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        SparkConf conf = new SparkConf().setAppName("sameHosts").setMaster("local[1]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> julyFirstLogs = sparkContext.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = sparkContext.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> julyFirstHosts = julyFirstLogs.map(getHost()).distinct();
        JavaRDD<String> augustFirstHosts = augustFirstLogs.map(getHost()).distinct();

        JavaRDD<String> commonHosts = julyFirstHosts.intersection(augustFirstHosts).filter(host -> !host.equals("host"));

        commonHosts.saveAsTextFile("out/nasa_logs_same_hosts.csv");
    }

    private static Function<String, String> getHost() {
        return log -> log.split("\t")[0];
    }
}
