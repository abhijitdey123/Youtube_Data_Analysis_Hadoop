import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelatedVideosMapper extends Mapper<LongWritable,Text, Text,IntWritable> {

	private Text id = new Text();
	private IntWritable relatedCount = new IntWritable();
	int count =0;
	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line= value.toString();
		String str[]= line.split("\t");
		
		if(str.length>9){
			count=str.length-9;  // Since the remaining number of columns have to be X -9 (9th is the comments Column)
			id.set(str[0]);
			relatedCount.set(count);
		}
		context.write(id, relatedCount);
	}
}
