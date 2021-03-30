package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;


import java.io.Serializable;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AverageHousePriceProblem {

	public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           output the average price for houses with different number of bedrooms.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it. 

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

           (3, 325000)
           (1, 266356)
           (2, 325000)
           ...

           3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
         */
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("averageHousePrice").setMaster("local[*]");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		JavaRDD<String> houses = sparkContext.textFile("in/RealEstate.csv");
		JavaRDD<String> cleanedLines = houses.filter(line -> !line.contains("Bedrooms"));

		JavaPairRDD<String, HouseCount> bedroomsAndPrices = cleanedLines.mapToPair(line -> {
			String[] splits = line.split(",");
			return new Tuple2<>(splits[3], new HouseCount(1, Double.parseDouble(splits[2])));
		});

		JavaPairRDD<String, HouseCount> bedroomsWithAllPrices = bedroomsAndPrices
			.reduceByKey((x, y) -> new HouseCount(
				x.count + y.count,
				x.totalPrice + y.totalPrice));

		for (Map.Entry<String, HouseCount> housePriceTotal : bedroomsWithAllPrices.collectAsMap().entrySet()) {
			System.out.println(housePriceTotal.getKey() + " : " + housePriceTotal.getValue());
		}

		JavaPairRDD<String, Double> averagePrices = bedroomsWithAllPrices.mapValues(house -> house.totalPrice / house.count);

		System.out.println("House Average Price:");
		for (Map.Entry<String, Double> housePriceAverage : averagePrices.collectAsMap().entrySet()) {
			System.out.println(housePriceAverage.getKey() + " : " + housePriceAverage.getValue());
		}
	}

	private static class HouseCount implements Serializable {
		private int count;
		private double totalPrice;

		HouseCount(int count, double totalPrice) {
			this.count = count;
			this.totalPrice = totalPrice;
		}

		@Override
		public String toString() {
			return "HouseCount{" +
				"count=" + count +
				", totalPrice=" + totalPrice +
				'}';
		}
	}

}
