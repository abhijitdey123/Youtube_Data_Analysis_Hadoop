import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text video_id_uploader = new Text();
	private IntWritable days = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] str = line.split("\t");

		if (str.length > 4) { // TO Remove Array Index of Bounds
				
			if (str[2].matches("\\d+") && Integer.parseInt(str[2])!=0) {  // 0 value is Invalid
				video_id_uploader.set(str[0] + "\t" + str[1]+"\t"+str[3]);
				days.set(Integer.parseInt(str[2]));
			}

		}

		context.write(video_id_uploader, days);

	}

}
