package com.hadoop.mr.simple.wordcount.test;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;

import com.hadoop.mr.simple.wordcount.WCMapper;
import com.hadoop.mr.simple.wordcount.WCReducer;


public class WordCountTest {

	MapDriver<LongWritable,Text ,Text ,LongWritable> mapDriver ;
	ReduceDriver reduceDriver;
	MapReduceDriver mapReduceDriver;



	@Before
	public void setup()
	{
		WCMapper wcMapper = new WCMapper();
		WCReducer wcReducer = new WCReducer();
		
		mapDriver = MapDriver.newMapDriver(wcMapper);
		reduceDriver = ReduceDriver.newReduceDriver(wcReducer);
		mapReduceDriver =  MapReduceDriver.newMapReduceDriver(wcMapper, wcReducer);
		

	}	





}
