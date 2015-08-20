package com.hadoop.mr.simple.wordcount.test;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;

import com.hadoop.mr.simple.wordcount.WCMapper;
import com.hadoop.mr.simple.wordcount.Reducer;


public class WordCountTest {

	MapDriver<LongWritable,Text , Text, LongWritable> mapDriver ;
	ReduceDriver< Text, LongWritable[], Text, LongWritable > reduceDriver ;
	MapReduceDriver mapReduceDriver 	;



	@Before
	public void setup()
	{
		WCMapper mapper = new WCMapper();
		Reducer reducer = new Reducer();
		
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		

	}	





}
