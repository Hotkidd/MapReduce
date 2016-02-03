package hadooplab1;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CommonWords {
	
	private static final String PATH_PREFIX = "hdfs://localhost:9000";
	private static URI PATH_URI_PREFIX;
	static{
		try{
			PATH_URI_PREFIX = new URI(PATH_PREFIX);
		}catch (URISyntaxException e){
			e.printStackTrace();
		}
	}

	// tokenize file 1
	public static class TokenizerWCMapper1 extends 
			Mapper<Object, Text, Text, Text> {

		Set<String> stopwords = new HashSet<String>();

		@Override
		protected void setup(Context context) {
			//Read stopwords from HDFS and put it into the Set<String> stopwords
			Configuration conf = context.getConfiguration();
			try{
				Path path = new Path(PATH_URI_PREFIX + "/data/input/sw4.txt");
				FileSystem fs = FileSystem.get(PATH_URI_PREFIX, new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String word = null;
				while((word=br.readLine())!=null){
					stopwords.add(word);
				}
			}catch(IOException e){
				e.printStackTrace();
			}
			
		}

		private Text word = new Text();
		private final static Text identifier = new Text("f1");

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				//check whether the word is in stopword list
				if(stopwords.contains(word.toString()))
					continue;
				//write out <word, f1>
				context.write(word,identifier);
				//System.out.println("mapper1 word:"+"\n" + word.toString());
				//System.out.println("mapper1 identifier:"+"\n" + identifier.toString());
			}
		}
	}

	// tokenize file 2
	public static class TokenizerWCMapper2 extends
			Mapper<Object, Text, Text, Text> {

		Set<String> stopwords = new HashSet<String>();

		@Override
		protected void setup(Context context) {
			//Read stopwords from HDFS and put it into the Set<String> stopwords
			Configuration conf = context.getConfiguration();
			try{
				Path path = new Path(PATH_URI_PREFIX + "/data/input/sw4.txt");
				FileSystem fs = FileSystem.get(PATH_URI_PREFIX, new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String word = null;
				while((word=br.readLine())!=null){
					stopwords.add(word);
				}
			}catch(IOException e){
				e.printStackTrace();
			}
		}

		private Text word = new Text();
		private final static Text identifier = new Text("f2");

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				//check whether the word is in stopword list
				if(stopwords.contains(word.toString()))
					continue;
				//write out <word, f2>
				context.write(word,identifier);
			}
		}
	}

	// get the number of common words
	public static class CommonWordsReducer extends
			Reducer<Text, Text, Text, IntWritable> {

		private IntWritable commoncount = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//maintain two counts for file 1 and file 2
			int count1 = 0;
			int count2 = 0;
			for (Text val : values) {
				// increase count1 or count2
				if(val.toString().equals("f1"))
					count1++;
				if(val.toString().equals("f2"))
					count2++;

			}
			if (count1 != 0 && count2 != 0) {
				//It is a common word here. Output its name and count
				int smallerCount = 0;
				if(count1<=count2)
					smallerCount = count1;
				else
					smallerCount = count2;
				commoncount.set(smallerCount);
				context.write(key, commoncount);
				//System.out.println("reducer key:"+"\n" + key.toString());
				//System.out.println("reducer count:"+"\n" + commoncount.toString());
				
			}
		}
	}

	public static class IntDescComparator extends WritableComparator{
		protected IntDescComparator(){
			super(IntWritable.class, true);
		}
		@SuppressWarnings("unchecked")
		@Override
		public int compare(WritableComparable a, WritableComparable b){
			return ((IntWritable)b).compareTo((IntWritable)a);
		}
	}
	public static class SortMapper extends
			Mapper<Text, Text, IntWritable, Text> {

		private IntWritable count = new IntWritable();

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			//output the <word, count> as <count, word>
			String str = value.toString();
			count.set(Integer.parseInt(str));
			context.write(count, key);
		}
	}

	public static class SortReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			//write out <key (word frequency) and value (word)>
			for (Text val : values){
				context.write(key, val);
			}

		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// Provide filenames
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err
					.println("Usage: TopKCommonWords <input1> <input2> <output1> "
							+ "<output2>");
			System.exit(2);
		}

		Path Input1 = new Path(PATH_PREFIX + otherArgs[0]);
		Path Input2 = new Path(PATH_PREFIX + otherArgs[1]);
		Path Output1 = new Path(PATH_PREFIX + otherArgs[2]);
		Path Output2 = new Path(PATH_PREFIX + otherArgs[3]);
		//Configure job 1 for counting common words
		Job job1 = new Job(conf, "Count Commond Words");
		job1.setJarByClass(CommonWords.class);
		//set input1 and input2 as input and <output1> as output for this MapReduce job.
		MultipleInputs.addInputPath(job1, Input1,
				TextInputFormat.class, TokenizerWCMapper1.class);
		MultipleInputs.addInputPath(job1, Input2,
				TextInputFormat.class, TokenizerWCMapper2.class);

		
		//set Mapper and Reduce class, output key, value class
		//job1.setCombinerClass(CommonWordsReducer.class);
		job1.setReducerClass(CommonWordsReducer.class);
		//job1.setMapOutputKeyClass(Text.class);
		//job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		//job1.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job1, Output1);
		job1.waitForCompletion(true);
		
		//Configure job 2 for sorting
		Job job2 = new Job(conf, "sort");
		job2.setJarByClass(CommonWords.class);
		
		//set input and output for MapReduce job 2 here
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    FileInputFormat.addInputPath(job2, Output1);
		FileOutputFormat.setOutputPath(job2, Output2);
		
		//job2.setSortComparatorClass(cls);
		
		//set Mapper and Reduce class, output key, value class 
		job2.setSortComparatorClass(IntDescComparator.class);
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job2, Output2);
	    
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
