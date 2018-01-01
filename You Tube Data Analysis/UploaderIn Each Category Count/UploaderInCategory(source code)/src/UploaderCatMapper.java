import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UploaderCatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text video_id = new Text();
	private IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		String str[] = line.split("\t");
		if (str.length > 4) {
			video_id.set(str[1] + "\t" + str[3]);

		}
		context.write(video_id, one);

	}

}
