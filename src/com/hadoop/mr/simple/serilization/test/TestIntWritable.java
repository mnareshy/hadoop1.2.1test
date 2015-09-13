package com.hadoop.mr.simple.serilization.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import com.hadoop.mr.simple.serilization.IntSerilize;

public class TestIntWritable {

	@Test
	public void testSerilize() throws IOException {
		
		IntWritable intWritable = new IntWritable();
		intWritable.set(143);
		
		byte[] bytes = IntSerilize.serilize(intWritable);
		
		assertEquals(bytes.length, 4);
		
	}
	
	@Test
	public void testDeserilize() throws IOException {
		
		IntWritable intWritable = new IntWritable();
		intWritable.set(143);
		byte[] bytes = new byte[4];  	
		
		byte[] bytes1 = IntSerilize.deserilize(intWritable, bytes);
		
		assertEquals(bytes.length, 4);
		//assertEquals(intWritable.get(), 143);
		
	}

}
