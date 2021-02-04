package Lab2;
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

import com.google.common.collect.Iterables;
        
public class Exercise2 {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String word = value.toString();
        
        char[] wordArray = word.toCharArray();
        Arrays.sort(wordArray);
        
        String wordSorted = new String(wordArray);
        
        context.write(new Text(wordSorted), new Text(word));
        
    }
 } 
        
 public static class Reduce extends Reducer<Text, Text, IntWritable, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
    	
    	int numWords = 0;
    	String wordSequence = "";
    	for (Text value : values) {
    		numWords++;
    		wordSequence += value.toString() + " ";
    	}

    	context.write(new IntWritable(numWords), new Text(wordSequence));
    	
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = Job.getInstance(conf, "exercise2");
    job.setJarByClass(Exercise2.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    
    job.setMapperClass(Map.class);
    // job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/csdeptucy/Lab_2/Exercise_2/words"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/csdeptucy/Lab_2/Exercise_2/out"));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
        
}