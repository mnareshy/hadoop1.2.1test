package com.hadoop.mr.simple.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountNonInner {

	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String[] myArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		
		Job job = new Job(conf, "wordcount");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(WCMapper.class);
		job.setReducerClass(Reducer.class);

		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(myArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(myArgs[1]));
		
		job.setJarByClass(WordCountNonInner.class);
		job.waitForCompletion(true);
	}


}


