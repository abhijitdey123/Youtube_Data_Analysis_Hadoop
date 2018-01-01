import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context con) throws IOException, InterruptedException {

		int sum = 0;
		int l = 0;

		for (IntWritable val : value) {
			sum = sum + val.get();
			l++;
		}
		sum = sum / l;
		con.write(key, new IntWritable(sum));

	}

}
