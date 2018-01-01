import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopViewMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text video_id = new Text();
	private IntWritable view = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		

			String str[] = line.split("\t");
			if(str.length>5){
			video_id.set(str[0]+"\t"+str[1]);
			if (str[5].matches("\\d+")) {
				int f = Integer.parseInt(str[5]);

				view.set(f);
			}

		}
		context.write(video_id, view);

	}

}
