package com.hadoop.mr.simple.serilization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IntSerilize {
	
	/*
	 * This class returns hadoop writable as byte array
	 * there will be a assiciated junit with this which will check 4 bytes in the array for a integer.
	 * 
	 */
	
	public static byte[] serilize(Writable writable) throws IOException 
	{
		ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
		DataOutputStream dataOutputStream = new DataOutputStream(byteArray);
		
		writable.write(dataOutputStream);
		
		dataOutputStream.close();
		
		return byteArray.toByteArray();
		
	}
	
	public static byte[] deserilize(Writable writable , byte[] bytes) throws IOException
	{
		ByteArrayInputStream bArrayInputStream = new ByteArrayInputStream(bytes);
		DataInputStream dInputStream = new DataInputStream(bArrayInputStream);
		
		writable.readFields(dInputStream);
		dInputStream.close();
		
		
		return bytes;
	}
	
	
	

}
