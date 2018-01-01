import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingsMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	private Text video_id = new Text();
	private FloatWritable ratings = new FloatWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		String str[] = line.split("\t");
		if (str.length > 6) {
			video_id.set(str[0]+"\t"+str[1]+"\t"+str[3]);
			if (str[6].matches("\\d+.+") || str[6].matches("\\d+") ) {
				float f = Float.parseFloat(str[6]);
					ratings.set(f);
			}

		}
		context.write(video_id, ratings);

	}

}
