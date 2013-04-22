import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SimplePR {
	
	static enum MyCounters { TOTAL_EDGES }
	
	// Compute filter parameters from jwb279
	static double fromNetID = 0.972;
	static double rejectMin = 0.99 * fromNetID;
	static double rejectLimit = rejectMin + 0.01;
	
	private static boolean selectInputLine(double x) { 
		return ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
	}

public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		// Split the line into a string array with the source node, destination node, and random floating point number
		String[] tempLine = new String[3];
		int i = 0;
		while (tokenizer.hasMoreTokens()) {
			tempLine[i] = tokenizer.nextToken();
			i++;
		}

		if (selectInputLine(Double.parseDouble(tempLine[2]))) {
			reporter.incrCounter(MyCounters.TOTAL_EDGES, 1);
			word.set(line);
			output.collect(word, one);
		}
	}
}

public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		output.collect(key, new IntWritable(sum));
	}
}

public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(SimplePR.class);
	conf.setJobName("Simple PageRank");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	}
}

