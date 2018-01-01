import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommentDriver {

	public static void main(String arg[]) throws Exception{
		if(arg.length!=2){
			System.err.println("Not enough Arguments");
			System.exit(1);
		}
	
		Job job=new Job();
		job.setJarByClass(CommentDriver.class);
		job.setJobName(" Top Sorted Comments List");
		
		FileInputFormat.addInputPath(job,new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		
		job.setMapperClass(CommentMapper.class);
		job.setReducerClass(CommentReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	
}
