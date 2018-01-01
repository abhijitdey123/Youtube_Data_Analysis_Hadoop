import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class CommentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text uploader_video_name = new Text();
	private IntWritable comments = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context con)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] str = line.split("\t");

		if (str.length > 8) {
			uploader_video_name.set(str[0] + "\t" + str[1]);
			if (str[8].matches("\\d+")) {
				int f = Integer.parseInt(str[8]);
				comments.set(f);
			}
		}

		con.write(uploader_video_name, comments);

	}

}