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
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

public class TopAirportPerAirport extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopAirportPerAirport(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/project/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Top Airport Per Airport");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(AirportAirportCountMap.class);
        jobA.setReducerClass(AirportAirportCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopAirportPerAirport.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Airport Per Airport");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(DoubleWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopAirportAirportMap.class);
        jobB.setReducerClass(TopAirportAirportReduce.class);
        jobB.setNumReduceTasks(1);
        
        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopAirportPerAirport.class);
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

    public static class AirportAirportCountMap extends Mapper<Object, Text, Text, IntWritable> {
    	String delimiters = ",";
    	List<String> queryAirports = Arrays.asList("CMI", "BWI", "MIA", "LAX", "IAH", "SFO");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String line         = value.toString();
        	String[] row        = line.split(delimiters);
        	String origAirport  = row[11].substring(1, row[11].length()-1);
        	String destAirport  = row[18].substring(1, row[18].length()-1);
        	String depDelayStr   = "xxx";
			
        	if (row[27].contains(".")) {
        		depDelayStr = row[27].substring(0, row[27].indexOf("."));
        	} 
        	
        	if (isInteger(depDelayStr) && queryAirports.contains(origAirport)) {
        		context.write(new Text(origAirport + "-" + destAirport), new IntWritable(Integer.parseInt(depDelayStr)));
        	}
        }
    }

    public static class AirportAirportCountReduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
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

    public static class TopAirportAirportMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N = 10;
        private TreeSet<Pair<Double, String>> airportAirportMap = new TreeSet<Pair<Double, String>>();
        private String lastAirport = "";

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	String keyStr      = key.toString();
        	
        	String  origAirport = keyStr.split("-")[0];
        	String  destAirport = keyStr.split("-")[1];
        	Double  time        = Double.parseDouble(value.toString());
        	
        	if (origAirport.equals(lastAirport)) {
        		// If the currentAirport is same with lastAirport
        		// Still use same container, keep the size of it
        		airportAirportMap.add(new Pair<Double, String>(time, origAirport+"-"+destAirport));
                if (airportAirportMap.size() > N) {
                	airportAirportMap.remove(airportAirportMap.last());
                }
        	} else {
        		// If the currentAirport is NOT same with lastAirport
        		
        		// Emit all the records of lastAirport from container
        		for (Pair<Double, String> item : airportAirportMap) { 
            		String[] strings = {item.second, item.first.toString()};
            		TextArrayWritable val = new TextArrayWritable(strings);
            		context.write(NullWritable.get(), val);
            	}
        		
        		// Reset the container
        		airportAirportMap.clear();
        		
        		// Update lastAirport
        		lastAirport = origAirport;
        		
        		// Add first new record into the container
        		airportAirportMap.add(new Pair<Double, String>(time, origAirport+"-"+destAirport));
        	}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	// Emit the data set of the last airport (last group)
        	for (Pair<Double, String> item : airportAirportMap) { 
        		String[] strings = {item.second, item.first.toString()};
        		TextArrayWritable val = new TextArrayWritable(strings);
        		context.write(NullWritable.get(), val);
        	}
        }
    }

    public static class TopAirportAirportReduce extends Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {
        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        	for (TextArrayWritable val: values) { 
        		Text[] pair    = (Text[]) val.toArray();
        		String airportAirport = pair[0].toString();
        		Double time           = Double.parseDouble(pair[1].toString());
        		context.write(new Text(airportAirport), new DoubleWritable(time));
        	}
        }
    }
}

class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>>
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