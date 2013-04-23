import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SimplePR2 {
	
	static enum MyCounters { TOTAL_EDGES }
	
	static final double INITIAL_PR = 1.0;
	static final double DAMPING = 0.85;
	
	// Compute filter parameters from jwb279
	static double fromNetID = 0.972;
	static double rejectMin = 0.99 * fromNetID;
	static double rejectLimit = rejectMin + 0.01;
	
	private static boolean selectInputLine(double x) { 
		return ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
	}

public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
	private Text nodeHead = new Text();

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		// tempLine will contain 1) Node 2) Old pagerank value for that node 3) List nodes it links to (outlinks)
		String[] tempLine = new String[3];
		int i = 0;
		while (tokenizer.hasMoreTokens()) {
			tempLine[i] = tokenizer.nextToken();
			i++;
		}
		
		nodeHead.set(tempLine[0]);		
		double oldPR = Double.parseDouble(tempLine[1]);
		String linkString = tempLine[2];
		String[] outLinks = linkString.split("\\|");
		double degree = outLinks.length;
		
		double prDivDegree = oldPR / degree;
		/*System.out.println("Head: " + nodeHead.toString());
		System.out.println("Degree: " + degree);
		System.out.println("prDivDegree: " + prDivDegree);*/
		output.collect(nodeHead, new Text("|" + linkString));
		//output.collect(nodeTail, new Text(nodeHead.toString() + "\t" + INITIAL_PR));
		
	}
}

public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		Text outputValue = new Text();
		String links = "";

		boolean first = true;
		while (values.hasNext()) {
			String tempStr = values.next().toString();
			if (tempStr.startsWith("|")) {
				if (first) {
					links += tempStr.substring(1);
				} else {
					links += tempStr;
				}
				first = false;
				continue;
			}
			
		}

		//links = links.replaceFirst("|", "");
		outputValue.set(Double.toString(INITIAL_PR) + "\t" + links);
		output.collect(key, outputValue);
	}
}

public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(SimplePR2.class);
	conf.setJobName("Simple PageRank2");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);

	conf.setMapperClass(Map.class);
	//conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	}
}

