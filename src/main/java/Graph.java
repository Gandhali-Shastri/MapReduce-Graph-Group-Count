import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Graph {
	public static class MyMapper extends Mapper<Object,Text,LongWritable,LongWritable> {
		@Override
		public void map ( Object key, Text value, Context context )
				throws IOException, InterruptedException {
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			long key2 = s.nextLong();
//			System.out.println(key2);
			long value2 = s.nextLong();
//			System.out.println(value2);
			context.write(new LongWritable(key2),new LongWritable(value2));
			s.close();
		}       

	}

	public static class MyReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
		@Override
		public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
				throws IOException, InterruptedException {

			long count = 0;
			for (LongWritable v: values) {                
				count++;
			};
			context.write(key,new LongWritable(count));
		}
	}

	public static class MyMapper2 extends Mapper<Object,LongWritable,LongWritable,LongWritable> {
		@Override
		public void map ( Object key, LongWritable value, Context context )
				throws IOException, InterruptedException {
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			long count = s.nextLong();

			context.write(new LongWritable(count),new LongWritable(1));
			s.close();
		}       

	}

	public static class MyReducer2 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
		@Override
		public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
				throws IOException, InterruptedException {

			long sum = 0;

			for (LongWritable v: values) {
				
				sum += v.get();
//				System.out.println("sum" + sum);
			};
			context.write(key,new LongWritable(sum));
		}
	}

	public static void main ( String[] args ) throws Exception {
		Configuration conf = new Configuration();

		Job job1 = Job.getInstance();
		
		job1.setJobName("MyJob1");
		job1.setJarByClass(Graph.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(LongWritable.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(LongWritable.class);
		job1.setMapperClass(MyMapper.class);
		job1.setReducerClass(MyReducer.class);
		//ip as text file
		job1.setInputFormatClass(TextInputFormat.class);
		//job1.setOutputFormatClass(TextOutputFormat.class);
		//output as binary
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(job1,new Path(args[0]));
		//saving binary op in temp folder
		FileOutputFormat.setOutputPath(job1,new Path("temp/"+args[1]));
		job1.waitForCompletion(true);

		boolean success = job1.waitForCompletion(true);
		//If 1st job is successful, run 2nd
		if (success) {
			Job job2 = Job.getInstance();
			job2.setJobName("MyJob2");
			job2.setJarByClass(Graph.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(LongWritable.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(LongWritable.class);
			job2.setMapperClass(MyMapper2.class);
			job2.setReducerClass(MyReducer2.class);
			//ip as binary file which is op of 1st reducer
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			//op as txt file
			job2.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.setInputPaths(job2,new Path("temp/"+args[1]));
			FileOutputFormat.setOutputPath(job2,new Path(args[1]));
			job2.waitForCompletion(true);

			FileSystem hdfs = FileSystem.get(conf);

			// delete existing temp directory
			if (hdfs.exists(new Path("temp/"))) {
				hdfs.delete(new Path("temp/"), true);
			}
		}

	}
}
