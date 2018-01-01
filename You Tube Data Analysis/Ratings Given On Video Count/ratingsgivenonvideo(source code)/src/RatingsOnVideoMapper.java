import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingsOnVideoMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {

	private Text ratingsKey = new Text();
	private IntWritable ratingsCount= new IntWritable();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
			
			String line = value.toString();
			String str[] = line.split("\t");
			
			if(str.length >8){
				ratingsKey.set(str[0]+"\t"+str[1]+"\t"+str[3]);
				if(str[7].matches("\\d+")){
					int f = Integer.parseInt(str[7]);
					ratingsCount.set(f);
				}
			}
		context.write(ratingsKey, ratingsCount);
		
	}

}
