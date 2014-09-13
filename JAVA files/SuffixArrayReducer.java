import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SuffixArrayReducer extends
		Reducer<IntWritable, NullWritable, String, String> {
	String DNAString = "";
	TreeMap<String, Long> sort = new TreeMap<String, Long>();
	HashSet<String> id = new HashSet<String>();

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		Path[] DNAFile = new Path[0];
		try {
			DNAFile = DistributedCache.getLocalCacheFiles(conf);
			BufferedReader fis = new BufferedReader(new FileReader(
					DNAFile[0].toString()));
			String line = null;
			DNAString = "";
			while ((line = fis.readLine()) != null)
				DNAString += line;

		} catch (IOException ioe) {
			System.err.println("Caught exception while getting cached files: ");
		}

	}

	public void reduce(IntWritable key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		sort.put(DNAString.substring((int) key.get()), (long) key.get());
	}

	public void cleanup(Context context) {

		try {
			for (String dna : sort.keySet())
				context.write(Long.toString(sort.get(dna)), dna);
		} catch (Exception e) {
		}
	}
}