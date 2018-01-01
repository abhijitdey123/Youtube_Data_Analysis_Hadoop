import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LengthMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text video_Id_key = new Text();
	private IntWritable video_length = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] str = line.split("\t");

		if (str.length > 5) {
			video_Id_key.set(str[0] + "\t" + str[1] +"\t"+ str[3]);

			if (str[4].matches("\\d+")) {

				video_length.set(Integer.parseInt(str[4]));
			}

		}
		context.write(video_Id_key, video_length);
	}
}
