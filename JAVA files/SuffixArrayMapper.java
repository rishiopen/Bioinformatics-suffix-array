import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SuffixArrayMapper extends
		Mapper<LongWritable, Text, IntWritable, NullWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		Long startIndex = Long.parseLong(tokens[0]);
		Long endIndex = Long.parseLong(tokens[1]);
		for (long i = startIndex; i <= endIndex; ++i) {
			context.write(new IntWritable((int) i), NullWritable.get());
		}
	}
}
