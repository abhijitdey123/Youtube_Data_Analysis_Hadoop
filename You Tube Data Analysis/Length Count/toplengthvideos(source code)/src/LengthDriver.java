import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LengthDriver {
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		if(args.length!=2){
			System.err.println("Insufficient number of arguments");
		}
		
		Job job =new Job();
		job.setJarByClass(LengthDriver.class);
		job.setJobName("Calculating Long Videos");
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.setMapperClass(LengthMapper.class);
		job.setReducerClass(LengthReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit((job.waitForCompletion(true)?0:1));
	}
	

}
