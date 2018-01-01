import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AgeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context con) throws IOException, InterruptedException {

		int sum = 0;
		int i = 0; // for Duplicate Data

		for (IntWritable val : value) {
			sum = sum + val.get();
			i++;
		}

		sum = sum / i;
		con.write(key, new IntWritable(sum));
	}

}
