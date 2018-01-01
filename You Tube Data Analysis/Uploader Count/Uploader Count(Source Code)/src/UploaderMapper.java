import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UploaderMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text category = new Text();
	private IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String str[] = line.split("\t");

		if (str.length > 2) {

			category.set(str[1]);

			context.write(category, one);

		}

	}
}