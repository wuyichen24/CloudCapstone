import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class TestCsv {
	private static final Pattern SPACE = Pattern.compile(" ");
	public static void main(String[] args) throws Exception {

		String filepath = "/user/root/test.csv";

		SparkConf sparkConf = new SparkConf().setAppName("TestCsv");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> file = ctx.textFile(filepath, 1);

		JavaRDD<String> rows = file.flatMap(new FlatMapFunction<String, String>() {
			  public Iterable<String> call(String s) { 
			  		return Arrays.asList(s.split("\n")); 
			  }
		});
		
		JavaRDD<String> column = rows.flatMap(new FlatMapFunction<String, String>() {
			  public Iterable<String> call(String s) { 
			  		return Arrays.asList(s.split(",")[18]); 
			  }
		});
		
		JavaRDD<String> columnNoHeader = column.filter(new Function<String, Boolean>() {
			public Boolean call(String s) throws Exception {
				if(s.equals("\"DestCityName\"")) {
					return false;
				}
				return true;
			}
		});

		JavaPairRDD<String, Integer> valuePair = columnNoHeader.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> countPair = valuePair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		JavaPairRDD<Integer, String> swapedCountPair = countPair.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
				return item.swap();
	        }
		});
		
		JavaPairRDD<Integer, String> sortedSwapedCountPair = swapedCountPair.sortByKey();
		
//		class IntegerComparator implements Comparator<Integer> {
//			public int compare(Integer a, Integer b) {
//			return String.valueOf(a).compareTo(String.valueOf(b))
//			}
//		}
//		sortByKey(java.util.Comparator<K> comp)

		List<Tuple2<Integer, String>> output = sortedSwapedCountPair.collect();
		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		ctx.stop();
	}
}
