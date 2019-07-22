package hadoop3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
	// Set HDFS URI prefix
	private static final String PATH_PREFIX = "hdfs://localhost:9000";
	private static URI PATH_URI_PREFIX;
	static {
		try {
			PATH_URI_PREFIX = new URI(PATH_PREFIX);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
	private static final String EDGE = "EDGE:";
	// record the number of nodes;
	public static int count = 0;
	// Compute one iteration of PageRank.
	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();

		public void map(Object key, Text value, Context context) 
		throws IOException, InterruptedException {
		// parse an input line into page, pagerank, outgoing links
			String[] strings = value.toString().split("\\s+");
			String page = strings[0];
			String pageRank = strings[1];
			// We need to output both graph structure and the credit sent to links
			StringBuilder edgeBuilder = new StringBuilder(EDGE);
			if (strings.length > 2) {
				String outgoingCredit = String.valueOf(Double.parseDouble(pageRank) / (strings.length - 2));
				v.set(outgoingCredit);
				for (int i = 2; i < strings.length; i++) {
					String outgoingLink = strings[i];
					edgeBuilder.append(outgoingLink);
					if (i != strings.length - 1) {
						edgeBuilder.append("\t");
					}
					// Credit: for each outgoing link, output a pair (link,
					// pagerank/number of outgoing links)
					k.set(outgoingLink);
					context.write(k, v);
					System.out.println("k: "+ k.toString()+ "v:"+v.toString());
				}
			}
			// Graph structure: output a pair of (page, EDGE:+outgoing links)
			k.set(page);
			v.set(edgeBuilder.toString());
			context.write(k, v);
			System.out.println("page: "+ k.toString()+ "outgoing link:"+v.toString());
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
	private Text v = new Text();

	public void reduce(Text key, Iterable<Text> value, Context context) 
	throws IOException, InterruptedException {
		String outgoingLinks = "";
		double sum = 0;
		for (Text t : value) {
			String valueString = t.toString();
			if (valueString.startsWith(EDGE)) {
			// analyze values, if the value starts with EDGE:, then the phrase
			// after EDGE: are outgoing links
				outgoingLinks = valueString.substring(EDGE.length());
			} else {
			// sum up the values that do not start with EDGE: into a variable sum
				sum = sum + Double.parseDouble(valueString);
			}
		}

		// compute new pagerank as 0.15/count+0.85*sum (count is the total number of nodes)
		double pageRank = 0.15 / count + 0.85 * sum;


		// output (key, new pagerank + outgoing links)
		System.out.println(pageRank + "\t" + outgoingLinks);
		v.set(pageRank + "\t" + outgoingLinks);
		context.write(key, v);
			}
		}

		public static class Mapper2 extends Mapper<Object, Text, DoubleWritable, Text> {
			DoubleWritable pageRankKey = new DoubleWritable();
			Text pageValue = new Text();
			public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
				String[] strings = value.toString().split("\\s+");
				String page = strings[0];
				String pageRank = strings[1];
				
				//output the double value
				pageRankKey.set(Double.parseDouble(pageRank));
				pageValue.set(page);
				context.write(pageRankKey, pageValue);
			}
		}

		public static class Reducer2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
			public void reduce(DoubleWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
				//write out <key (page rank) and value (page)>
				for (Text val : values) {
					context.write(key, val);
				}
			}
		}
		// Comparator for sorting page rank in desc order.
		public static class DoubleDescComparator extends WritableComparator {
			protected DoubleDescComparator() {
				super(DoubleWritable.class, true);
			}
			@SuppressWarnings("rawtypes")
			@Override
			public int compare(WritableComparable a, WritableComparable b) {
				return ((DoubleWritable)b).compareTo((DoubleWritable)a);
			}
		}


		// Count the number of nodes and attach a pagerank score 1
		public static void preprocessing(String filename) {
			try {
				Path inputPath = new Path(PATH_PREFIX + filename);
				Path outputPath=new Path(PATH_PREFIX + "/data/output3/processed0.txt");
				FileSystem fs = FileSystem.get(PATH_URI_PREFIX, new Configuration());
				Scanner sc = new Scanner(fs.open(inputPath));
				PrintWriter pw = new PrintWriter(fs.create(outputPath));
				
				String line = new String();
				while (sc.hasNextLine()) {
					line = sc.nextLine();
					if (line==null) {
						continue;
					}
					count ++;
					String[] node = line.split("\t");
					StringBuilder newLine = new StringBuilder();
					newLine.append(node[0]+"\t"+"1");
					for(int i=1;i<node.length;i++){
						newLine.append("\t"+node[i]);
					}
					pw.println(newLine.toString());
				}
				sc.close();
				pw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public static void main(String[] args) throws IOException,
		InterruptedException, ClassNotFoundException {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args)
			.getRemainingArgs();
			if (otherArgs.length != 1) {
				System.err.println("Usage: PageRank <input1>");
				System.exit(2);
			}

			preprocessing(otherArgs[0]);

			int iteration = 10;
			for (int i = 0; i < iteration; i++) {
				Path input = new Path(PATH_PREFIX + "/data/output3/processed" + i + ".txt");
				Path output = new Path(PATH_PREFIX + "/data/output3/processed" + (i + 1) + ".txt");
				Job job = new Job(conf, "page rank");
				job.setJarByClass(PageRank.class);
				job.setInputFormatClass(TextInputFormat.class);
				FileInputFormat.addInputPath(job, input);
				FileOutputFormat.setOutputPath(job, output);
				job.setMapperClass(Mapper1.class);
				job.setReducerClass(Reducer1.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.waitForCompletion(true);
			}
			Path input = new Path(PATH_PREFIX + "/data/output3/processed" + iteration + ".txt");
			Path output = new Path(PATH_PREFIX + "/data/output3/processed" + (iteration + 1) + ".txt");
			Job job = new Job(conf, "sort");
			job.setJarByClass(PageRank.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);
			job.setSortComparatorClass(DoubleDescComparator.class);
			job.setMapperClass(Mapper2.class);
			job.setReducerClass(Reducer2.class);
			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(Text.class);
			job.waitForCompletion(true);
		}
}