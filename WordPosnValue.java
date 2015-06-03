package abc;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordPosnValue extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String position = "";
			String count = "";
			String wordPosn = "";
			String[] wordPosnSplit;
			String wordCount = "";
			String line = value.toString();
			String[] wordPosnCount = line.split("\t");
			
			if (wordPosnCount.length > 1) {
				
				count = wordPosnCount[1];
				wordPosn = wordPosnCount[0];
				wordPosnSplit = wordPosn.split(" ");
				position = wordPosnSplit[1];
				wordCount = wordPosnSplit[0] + " " + count;
				output.collect(new Text(position), new Text(wordCount));
				
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			int N = 0;
			String wordPosn = "";
			String wordCount = "";
			String[] wordCountSplit;
			List<String> array_list = new ArrayList<String>();
			double prob;
			float value1;
			int value2;

			while (values.hasNext()) {
				wordCount = values.next().toString();
				wordCountSplit = wordCount.split(" ");
				N += Integer.parseInt(wordCountSplit[1]);
				array_list.add(wordCount);
			}

			for (String s : array_list) {
				
				IntWritable i = new IntWritable();
				wordCountSplit = s.split(" ");
				value1 = Float.parseFloat(wordCountSplit[1]);
				value2 = N;
				prob = value1 / value2;
				Integer approxprob = (int) (prob * 10000);
				i.set(approxprob);

				wordPosn = wordCountSplit[0] + " " + key;
				output.collect(new Text(wordPosn), i);
			}

		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}