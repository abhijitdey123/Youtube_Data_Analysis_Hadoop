import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RatingsDriver {

	public static void main(String args[]) throws Exception {

		if (args.length != 2) {
			System.err.println("Error");
			System.exit(1);
		}

		Job job = new Job();
		job.setJarByClass(RatingsDriver.class);
		job.setJobName("Video Ratings");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(RatingsMapper.class);
		job.setReducerClass(RatingsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}
}
