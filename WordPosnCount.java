package abc;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;

public class WordPosnCount extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text word = new Text();
		private IntWritable position;

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			int count = 0;
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				System.out.println("In Map 1");
				count += 1;
				word.set(tokenizer.nextToken());
				position = new IntWritable(count);
				output.collect(word, position);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			List<IntWritable> array_list = new ArrayList<IntWritable>();
			HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>();
			String word;
			Text text = new Text();

			while (values.hasNext()) {
				int position = values.next().get();
				if (hm.containsKey(position)) {
					int positionCount = hm.get(position);
					positionCount = positionCount + 1;
					hm.put(position, positionCount);
				} else {
					hm.put(position, 1);
					array_list.add(new IntWritable(position));
				}
			}

			for (IntWritable index : array_list) {

				int temp = index.get();
				int val = hm.get(temp);
				System.out.println("In Reduce 1");
				word = "";
				word = key.toString();
				word = word + " " + index;
				text.set(word);
				output.collect(text, new IntWritable(val));
			}
			hm.clear();

		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}