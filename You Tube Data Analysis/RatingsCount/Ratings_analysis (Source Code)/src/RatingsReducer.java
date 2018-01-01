import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingsReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	protected void reduce(Text key, Iterable<FloatWritable> value, Context con) throws IOException, InterruptedException {

		float sum = 0;
		int l = 0;

		for (FloatWritable val : value) {
			sum = sum +  val.get();
			l++;
			}
		sum=sum/l;
		con.write(key, new FloatWritable(sum));

	}

}
