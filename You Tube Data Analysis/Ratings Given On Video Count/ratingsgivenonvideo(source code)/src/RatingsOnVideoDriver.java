import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RatingsOnVideoDriver {

	public static void main(String args[])throws Exception{
		
		if(args.length!=2){
			System.err.println("Not Enough Arguements");
			System.exit(1);
		}
		
		Job job=new Job();
		job.setJarByClass(RatingsOnVideoDriver.class);
		job.setJobName(" Ratings Given On Each Video Count");
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(RatingsOnVideoMapper.class);
		job.setReducerClass(RatingsOnVideoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}
	
	
	
}
