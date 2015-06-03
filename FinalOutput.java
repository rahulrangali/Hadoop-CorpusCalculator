package abc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FinalOutput extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		Text constant = new Text();
		Text text = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			constant.set("keyconstant");
			text.set(value.toString().trim());
			output.collect(constant, text);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, IntWritable> {

		Hashtable<String, Double> ht = new Hashtable<String, Double>();
		LinkedHashSet<String> sentences = new LinkedHashSet<String>();
		private Path[] corpus;

		public void configure(JobConf job) {
			try {

				String s;
				corpus = DistributedCache.getLocalCacheFiles(job);
				File file = new File(corpus[0].getName());
				BufferedReader br = new BufferedReader(new FileReader(file));
				while ((s = br.readLine()) != null) {

					sentences.add(s);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String[] compare1 = { "", "", "" };
			Double[] compare2 = { 0.0, 0.0, 0.0 };

			while (values.hasNext()) {
				String temp = values.next().toString().trim();
				temp.split("\t");
				ht.put(temp.split("\t")[0],
						Double.parseDouble(temp.split("\t")[1]));
			}
			System.out.println(ht);
			Iterator<String> itr = sentences.iterator();

			while (itr.hasNext()) {
				Double probsentence = 1.0;
				int wc = 1;
				String line = itr.next().toString().trim();
				String[] words = line.split(" +");
				for (String w : words) {
					Double prob = ht.get(w + " " + wc);
					probsentence *= ((double) prob / 10000);
					wc = wc + 1;
				}
				if (probsentence > compare2[0]) {
					compare2[2] = compare2[1];
					compare1[2] = compare1[1];

					compare2[1] = compare2[0];
					compare1[1] = compare1[0];

					compare2[0] = probsentence;
					compare1[0] = line;
				} else if (probsentence > compare2[1]) {
					compare2[2] = compare2[1];
					compare1[2] = compare1[1];

					compare2[1] = probsentence;
					compare1[1] = line;
				} else if (probsentence > compare2[2]) {
					compare2[2] = probsentence;
					compare1[2] = line;
				}
			}
			Text lines = new Text();
			for (int i = 0; i < 3; i++) {
				lines.set(compare1[i]);
				output.collect(lines, new IntWritable(10));
			}

		}
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}
