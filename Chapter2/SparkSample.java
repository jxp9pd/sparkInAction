package sparkSamples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class SparkSample
{
	private static Set<String> stopWords = new HashSet<>();

	static {
		stopWords.add("at");
		stopWords.add("if");
		stopWords.add("the");
		stopWords.add("a");
		stopWords.add("that");
		stopWords.add("no");
		stopWords.add("this");
		stopWords.add("any");
		stopWords.add("for");
		stopWords.add("in");
		stopWords.add("on");
		stopWords.add("of");
		stopWords.add("and");
		stopWords.add("to");
		stopWords.add("or");
		stopWords.add("is");
		stopWords.add("his");
	}
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Sample");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> rdd = sc.textFile("constituion.txt");
		System.out.println("Lines in RDD are " + rdd.count());
		
		JavaRDD<String> filtRdd = rdd.filter(x -> x.toLowerCase().contains("freedom"));
		System.out.println("Filtered RDD contains " + 
				filtRdd.count() + 
				" lines.");
		for (String line : filtRdd.collect()) {
			System.out.println("Line is " + line);
		}
		
		JavaRDD<String> flatMap = rdd.flatMap(new FlatMapFunction<String,String>() {
			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(",")).iterator();
			}			
		});
		
		JavaRDD<String> flatMapOther = rdd.flatMap(line -> Arrays.asList(line.split(",")).iterator());
		System.out.println("There are " + flatMapOther.count() + " words in the file.");
		
		
		// Word count in one line
		JavaPairRDD<String,Integer> wordCounts = rdd
			.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
			.mapToPair(word -> new Tuple2<String,Integer>(word, 1))
			.reduceByKey((x,y) -> x + y)
			.sortByKey();
		
		JavaPairRDD<String,Integer> wordCountsBetter = rdd.flatMap(line -> splitAndClean(line))
				.mapToPair(word -> new Tuple2<String,Integer>(word, 1))
				.reduceByKey((x,y) -> x + y)
				.mapToPair(x -> new Tuple2<Integer,String>(x._2, x._1))
				.sortByKey()
				.mapToPair(x -> new Tuple2<String,Integer>(x._2, x._1));
		
		for (Tuple2<String,Integer> pair : wordCountsBetter.collect()) {
			System.out.println("Word <" + pair._1 + "> appeared " + pair._2 + " times.");
		}
		sc.close();
	}

	private static Iterator<String> splitAndClean(String line) {
		List<String> words = Arrays.asList(line.split(" "));
		List<String> clean = new ArrayList<>();
		for (String word : words) {
			if (word.length() == 0 || isStopWord(word)) {
				continue;
			}
			String cword = stripChars(word.toLowerCase(), "\";,.[]()");
//			System.out.println("Before " + word + " after " + cword);
			clean.add(cword);
		}
		return clean.iterator();
	}

	private static boolean isStopWord(String word) {
		if (stopWords.contains(word))
			return true;
		return false;
	}

	private static String stripChars(String input, String strip) {
	    StringBuilder result = new StringBuilder();
	    for (char c : input.toCharArray()) {
	        if (strip.indexOf(c) == -1) {
	            result.append(c);
	        }
	    }
	    return result.toString();
	}
}
