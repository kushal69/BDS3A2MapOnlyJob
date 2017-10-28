package com;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 

public class Task1Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		
		Text companyName = new Text(lineArray[0]);
		Text productName = new Text(lineArray[1]);

		if(!(companyName.toString().equalsIgnoreCase("NA")) &&
				!(productName.toString().equalsIgnoreCase("NA"))) {
			context.write(NullWritable.get(), value);
		}
	}
}
