package com;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//	Main Class to run the task
public class Task1 {
	@SuppressWarnings("deprecation")
	//Main Method which creates the job
	public static void main(String[] args) throws Exception {
		Job job = new Job();				//	Creating a Map job
		job.setJarByClass(Task1.class);		//	Setting the Jar name
		job.setJobName("Filter");			//	Giving the Job Name
		
		job.setMapperClass(Task1Mapper.class);	//	Setting the Mapper Class
		job.setNumReduceTasks(0);				//	Setting the No. Of Reducers

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); //File input path
		FileOutputFormat.setOutputPath(job,new Path(args[1]));//File output path
		
		job.waitForCompletion(true);
	}
}
