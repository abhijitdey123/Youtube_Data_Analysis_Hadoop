import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelatedVideosReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text id, Iterable<IntWritable>value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context con) throws IOException, InterruptedException {
		
		int sum=0,l=0;
		
		for(IntWritable val:value){
			sum+=val.get();
			l++;
		}
			sum=sum/l;
			con.write(id, new IntWritable(sum));		
	}
	

}
