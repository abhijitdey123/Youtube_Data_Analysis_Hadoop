import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UploaderCatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context con) throws IOException, InterruptedException {

		int sum = 0;
		int l = 0;

		for (IntWritable val : value) {
			sum = sum + val.get();

		}

		con.write(key, new IntWritable(sum));

	}

}
