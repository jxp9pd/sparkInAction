package sparkSamples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SquareReverse {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Sample");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		ArrayList<Integer> values = new ArrayList<Integer>();
		for (int i = 10; i < 20; i++) {
			values.add(i);
		}
		JavaRDD<Integer> numbers = sc.parallelize(values);
		JavaRDD<Integer>numbersSquared = numbers.map(num -> num * num);
		numbersSquared.foreach(x->System.out.println(x));
		
		JavaRDD<String> reversed = numbersSquared.map(x -> reverse(x.toString()));
		reversed.foreach(x ->System.out.println(x));
	}
	public static String reverse(String str) {
		String result = "";
		for (int i=str.length()-1; i>=0; i--) {
			result+=str.charAt(i);
		}
		return result;
	}
}
