package com.hadoop.mr.simple.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable[], Text, LongWritable>{

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