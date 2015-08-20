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

public class WordCount {

	public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable,Text , Text, LongWritable>{

		protected void map(LongWritable keyIn, Text vaueIn, Context context) throws IOException, InterruptedException{

			String value = vaueIn.toString();
			String wordsPerLine[] = value.split("//t");
			Text emitKey = new Text();
			LongWritable emitValue = new LongWritable(1);

			for (String word : wordsPerLine) {
				emitKey.set(word);
				context.write(emitKey, emitValue);

			}

		}

	}

	public  static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable[], Text, LongWritable>{

		protected void reduce(Text keyIn,Iterable<LongWritable> valueIn ,Context context ) throws IOException, InterruptedException{

			LongWritable emitValue = new LongWritable();
			long sum = 0;

			for (LongWritable value : valueIn) {

				sum = sum + value.get();

			}

			emitValue.set(sum);
			context.write(keyIn, emitValue);


		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String[] myArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		
		Job job = new Job(conf, "wordcount");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);

		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(myArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(myArgs[1]));
		
		job.setJarByClass(WordCount.class);
		job.waitForCompletion(true);
	}


}


