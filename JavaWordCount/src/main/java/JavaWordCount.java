import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	public static void main(String[] args) throws Exception {

		String filepath = "/user/root/textfile.txt";

		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(filepath, 1);

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			  public Iterable<String> call(String s) { 
			  		return Arrays.asList(s.split(" ")); 
			  }
		});
		
		words.collect();

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		JavaPairRDD<Integer, String> swappedCounts = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
				return item.swap();
	        }
		});
		
		JavaPairRDD<Integer, String> sortedCounts = swappedCounts.sortByKey();
		
//		class IntegerComparator implements Comparator<Integer> {
//			public int compare(Integer a, Integer b) {
//			return String.valueOf(a).compareTo(String.valueOf(b))
//			}
//		}
//		sortByKey(java.util.Comparator<K> comp)

		List<Tuple2<Integer, String>> output = sortedCounts.collect();
		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		ctx.stop();
	}
}
