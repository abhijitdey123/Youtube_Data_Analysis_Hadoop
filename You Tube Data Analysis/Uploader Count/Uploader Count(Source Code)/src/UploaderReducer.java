import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UploaderReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text category, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context con) throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable val : values) {
			sum += val.get();
		}

		con.write(category, new IntWritable(sum));
	}

}
