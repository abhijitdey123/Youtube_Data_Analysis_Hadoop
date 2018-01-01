import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AgeDriver {
	
	public static void main(String args[]) throws Exception{
		
		if(args.length!=2){
			System.err.println("Invalid Number of Arguments");
		}
		
		
		Job job = new Job();
		job.setJarByClass(AgeDriver.class);
		job.setJobName("Video Age Count");
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AgeMapper.class);
		job.setReducerClass(AgeReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
