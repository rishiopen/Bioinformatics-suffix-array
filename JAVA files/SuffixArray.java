import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapred.Counters.Counter;
import java.util.*;
import java.io.*;

public class SuffixArray {

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		Configuration conf = new Configuration();

		DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
		Job job = new Job(conf, "genomeName");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(SuffixArray.class);

		job.setMapperClass(SuffixArrayMapper.class);
		job.setReducerClass(SuffixArrayReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[3]));
		NLineInputFormat.setNumLinesPerSplit(job, 1);
		job.setInputFormatClass(NLineInputFormat.class);
		job.setPartitionerClass(SuffixArrayPartitioner.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(String.class);

		job.waitForCompletion(true);
		long finish = System.currentTimeMillis();
		System.out.println("Total time taken :: " + (finish - start));
	}
}