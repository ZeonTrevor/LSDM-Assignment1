import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class AverageHadoop {
    static class NumberMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static List<String> numbers = Arrays.asList("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int mapped = numbers.indexOf(value.toString());
            context.write(new Text("mapper"), new Text(Integer.toString(mapped)));
        }
    }

    //Important Property is that the input types of key/value pairs and output types of key/values should be same
    static class Combiner extends Reducer<Text, Text, Text, Text> {
    	  @Override
    	  protected void reduce(Text key, Iterable<Text> values,
    	    Context context) throws IOException, InterruptedException {
    	   Integer count = 0;
    	   Integer sum = 0;
    	   Iterator<Text> itr = values.iterator();
    	   while (itr.hasNext()) {
    		String value = itr.next().toString();
    	    Integer number = Integer.parseInt(value);
    	    count++;
    	    sum += number;
    	   }
    	   System.out.println("sum: " + sum);
    	   context.write(new Text("S_C"), new Text(sum + "_" + count));
    	  }
    }
    
    static class AverageReducer extends Reducer<Text, Text, NullWritable, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double sum = 0D;
            int totalCount = 0;
            Iterator<Text> itr = values.iterator();
            while (itr.hasNext()) {
        	    String value = itr.next().toString();
        	    String[] valueSplit = value.split("_");
        	    sum += Double.parseDouble(valueSplit[0]);
        	    totalCount += Integer.parseInt(valueSplit[1]);        	    
        	   
            }
            
            Double average = sum/totalCount;
            context.write(NullWritable.get(), new DoubleWritable(average));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(AverageHadoop.class);
        job.setNumReduceTasks(1);
        
        job.setMapperClass(NumberMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(Combiner.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path("data/numbers.txt"));
        TextOutputFormat.setOutputPath(job, new Path("results/hadoop/numberaverage"));

        job.waitForCompletion(true);
    }

}
