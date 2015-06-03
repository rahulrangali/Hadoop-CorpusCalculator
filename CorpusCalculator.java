package abc;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class CorpusCalculator {

	public static void main(String[] args) throws IOException,
			URISyntaxException {

		JobConf conf = new JobConf(WordPosnCount.class);
		conf.setJobName("WordPosnCount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(WordPosnCount.Map.class);
		conf.setReducerClass(WordPosnCount.Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]+"output1"));

		JobClient.runJob(conf);

		JobConf conf1 = new JobConf(WordPosnValue.class);
		conf1.setJobName("WordPosnValue");

		conf1.setMapOutputKeyClass(Text.class);
		conf1.setMapOutputValueClass(Text.class);

		conf1.setMapperClass(WordPosnValue.Map.class);
		conf1.setReducerClass(WordPosnValue.Reduce.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf1, new Path(args[1]+"output1/part-00000"));
		FileOutputFormat.setOutputPath(conf1, new Path(args[1]+"output2"));

		JobClient.runJob(conf1);

		JobConf conf2 = new JobConf(FinalOutput.class);
		conf2.setJobName("FinalOutput");

		conf2.setMapOutputKeyClass(Text.class);
		conf2.setMapOutputValueClass(Text.class);

		conf2.setMapperClass(FinalOutput.Map.class);
		conf2.setReducerClass(FinalOutput.Reduce.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);

		 //DistributedCache.addCacheFile(new URI("hdfs://localhost:54310/home/hduser1/Output/A65.txt"), conf2);
		//DistributedCache.addCacheFile(new File("Corpus.txt").toURI(), conf2);
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf2);
		FileInputFormat.setInputPaths(conf2, new Path(args[1]+"output2/part-00000"));
		FileOutputFormat.setOutputPath(conf2, new Path(args[1]+"output3"));

		JobClient.runJob(conf2);
	}

}
