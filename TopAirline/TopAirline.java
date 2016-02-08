import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeSet;

public class TopAirline extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopAirline(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/project/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Top Airline");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(AirlineCountMap.class);
        jobA.setReducerClass(AirlineCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopAirline.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Airline");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(DoubleWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopAirlineMap.class);
        jobB.setReducerClass(TopAirlineReduce.class);
        jobB.setNumReduceTasks(1);
        
        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopAirline.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }
    
    public static boolean isInteger(String str) {
	    if (str == null) {
	        return false;
	    }
	    int length = str.length();
	    if (length == 0) {
	        return false;
	    }
	    int i = 0;
	    if (str.charAt(0) == '-') {
	        if (length == 1) {
	            return false;
	        }
	        i = 1;
	    }
	    for (; i < length; i++) {
	        char c = str.charAt(i);
	        if (c < '0' || c > '9') {
	            return false;
	        }
	    }
	    return true;
	}

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static class AirlineCountMap extends Mapper<Object, Text, Text, IntWritable> {
    	String delimiters = ",";

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String line          = value.toString();
        	String[] row         = line.split(delimiters);
        	String Airline       = row[6].substring(1, row[6].length()-1);
        	String ArrDelayStr   = "xxx";
        	
        	if (row[38].contains(".")) {
        		ArrDelayStr = row[38].substring(0, row[38].indexOf("."));
        	}
        	
        	if (isInteger(ArrDelayStr)) {
        		context.write(new Text(Airline), new IntWritable(Integer.parseInt(ArrDelayStr)));
        	}
        }
    }

    public static class AirlineCountReduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum  = 0;
        	int size = 0;
			for (IntWritable val : values) {
				sum += val.get();
				size = size + 1;
			}
			double avg = (double) sum / (double) size;
			context.write(key, new DoubleWritable(avg));
        }
    }

    public static class TopAirlineMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N = 10;
        private TreeSet<Pair<Double, String>> countAirlineMap = new TreeSet<Pair<Double, String>>();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	String Airline = key.toString();
        	Double count = Double.parseDouble((value.toString()));
            countAirlineMap.add(new Pair<Double, String>(count, Airline));
            if (countAirlineMap.size() > N) {
                countAirlineMap.remove(countAirlineMap.last());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Pair<Double, String> item : countAirlineMap) { 
        		String[] strings = {item.second, item.first.toString()};
        		TextArrayWritable val = new TextArrayWritable(strings);
        		context.write(NullWritable.get(), val);
        	}
        }
    }

    public static class TopAirlineReduce extends Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {
        Integer N = 10;
        private TreeSet<Pair<Double, String>> countAirlineMap = new TreeSet<Pair<Double, String>>();

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        	for (TextArrayWritable val: values) { 
        		Text[] pair= (Text[]) val.toArray();
        		String Airline = pair[0].toString();
        		Double count = Double.parseDouble(pair[1].toString());
        		countAirlineMap.add(new Pair<Double, String>(count, Airline));
        		if (countAirlineMap.size() > N) {
        			countAirlineMap.remove(countAirlineMap.last());
        		} 
        	}

        	for (Pair<Double, String> item: countAirlineMap) { 
        		Text word = new Text(item.second);
        		DoubleWritable value = new DoubleWritable(item.first); 
        		context.write(word, value);
        	}
        }
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
