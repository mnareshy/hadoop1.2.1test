package com.hadoop.mr.simple.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable,Text , Text, LongWritable>{

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