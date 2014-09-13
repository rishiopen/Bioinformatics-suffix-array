import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.DistributedCache;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;

class SuffixArrayPartitioner<V extends Writable> extends
		Partitioner<IntWritable, NullWritable> implements Configurable {
	static String DNA;
	private Configuration conf;
	static int Acount;
	static int Ccount;
	static int Gcount;
	static int Tcount;
	static int total;
	static int CAbove;
	static int GAbove;
	static int TAbove;
	static int Dcount;
	static int AAbove;
	static boolean flag = false;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		if (!flag) {
			flag = true;
			this.conf = conf;
			JobClient job = new JobClient();
			Path[] DNAFile = new Path[1];
			try {
				DNAFile = DistributedCache.getLocalCacheFiles(conf);
				BufferedReader fis = new BufferedReader(new FileReader(
						DNAFile[0].toString()));
				String line = null;
				DNA = "";
				Acount = 0;
				Ccount = 0;
				Gcount = 0;
				Tcount = 0;
				total = 0;
				while ((line = fis.readLine()) != null) {

					DNA += line;
					Acount += SuffixArrayStringUtils.countMatches(line, "A");
					Ccount += SuffixArrayStringUtils.countMatches(line, "C");
					Gcount += SuffixArrayStringUtils.countMatches(line, "G");
					Tcount += SuffixArrayStringUtils.countMatches(line, "T");
					Dcount += SuffixArrayStringUtils.countMatches(line, "$");

				}

				total = Dcount + Acount + Ccount + Gcount + Tcount;
				AAbove = Acount + Dcount;
				CAbove = Acount + Ccount + Dcount;
				GAbove = Gcount + Acount + Ccount + Dcount;
				TAbove = Tcount + Ccount + Acount + Dcount + Gcount;
			}

			catch (Exception e) {
			}
		}
	}

	public int getPartition(IntWritable index, NullWritable value,
			int numPartitions) {

		int Index = index.get();
		int partition = getHashValue(Index, numPartitions);

		return partition;
	}

	public int getHashValue(int index, int numPartitions) {
		int perReducer = (int) Math.ceil(total / (numPartitions * 1.0));

		int d;
		switch (DNA.charAt(index)) {

		case 'A':
			AAbove -= 1;
			d = AAbove / perReducer;
			if (d < 0)
				return 0;
			break;
		case 'C':
			CAbove -= 1;
			d = CAbove / perReducer;
			if (d < 0)
				return 0;

			break;
		case 'G':
			GAbove -= 1;
			d = GAbove / perReducer;
			if (d < 0)
				return 0;

			break;
		case 'T':
			TAbove -= 1;
			d = TAbove / perReducer;
			if (d < 0)
				return 0;
			break;
		default:
			AAbove -= 1;
			return 0;

		}
		return d;
	}
}