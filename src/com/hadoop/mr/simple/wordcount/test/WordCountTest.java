package com.hadoop.mr.simple.wordcount.test;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.hadoop.mr.simple.wordcount.WCMapper;
import com.hadoop.mr.simple.wordcount.WCReducer;


public class WordCountTest {

	MapDriver<LongWritable,Text ,Text ,LongWritable> mapDriver ;
	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
	MapReduceDriver<LongWritable,Text ,Text ,LongWritable,Text ,LongWritable> mapReduceDriver;



	@Before
	public void setup()
	{
		WCMapper wcMapper = new WCMapper();
		WCReducer wcReducer = new WCReducer();

		mapDriver = MapDriver.newMapDriver(wcMapper);
		reduceDriver = ReduceDriver.newReduceDriver(wcReducer);
		mapReduceDriver =  MapReduceDriver.newMapReduceDriver(wcMapper, wcReducer);

	}	

	@Test
	public void testWCMapper() throws IOException
	{
		mapDriver.withInput(new LongWritable(),new Text( "  INCOMPATIBLE    CHANGES"));
		mapDriver.withOutput(new Text("INCOMPATIBLE"), new LongWritable(1));
		mapDriver.withOutput(new Text("CHANGES"), new LongWritable(1));
		mapDriver.runTest();

	}

	@Test
	public void testWCReducer() throws IOException
	{
		List<LongWritable> values1 = new ArrayList<LongWritable>();
		values1.add(new LongWritable(1));
		values1.add(new LongWritable(1));
		values1.add(new LongWritable(1));
		values1.add(new LongWritable(1));

		reduceDriver.withInput(new Text("INCOMPATIBLE"), values1);
		reduceDriver.withOutput(new Text("INCOMPATIBLE"), new LongWritable(4));

		reduceDriver.runTest();

	}
	
	





}
