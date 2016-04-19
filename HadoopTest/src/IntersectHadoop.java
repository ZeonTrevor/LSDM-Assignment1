
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class IntersectHadoop {

	static class KeyValueWritable implements WritableComparable<KeyValueWritable>
	{

		private Text key;
		private IntWritable value;

		//Default Constructor
		public KeyValueWritable() 
		{
			this.key = new Text();
			this.value = new IntWritable();
		}

		//Custom Constructor
		public KeyValueWritable(Text key, IntWritable value) 
		{
			this.key = key;
			this.value = value;
		}

		public void set(Text key, IntWritable value) 
		{
			this.key = key;
			this.value = value;
		}

		public Text getKey()
		{
			return key; 
		}

		public IntWritable getValue()
		{
			return value; 
		}
		@Override
		public int compareTo(KeyValueWritable o) 
		{
			if (key.compareTo(o.key)==0)
			{
				return (value.compareTo(o.value));
			}
			else return (key.compareTo(o.key));
		}

		@Override
		public boolean equals(Object o) 
		{
			if (o instanceof KeyValueWritable) 
			{
				KeyValueWritable other = (KeyValueWritable) o;
				return key.equals(other.key) && value.equals(other.value);
			}
			return false;
		}

		@Override
		public int hashCode()
		{
			return key.hashCode();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			key.readFields(in);
			value.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			key.write(out);
			value.write(out);

		}
	}
	
	static class LineMapper extends Mapper<LongWritable, Text, Text, KeyValueWritable> {
		@Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String fileName = context.getInputSplit().toString();
            context.write(new Text(line[0]), new KeyValueWritable(new Text(fileName), new IntWritable(Integer.parseInt(line[1]))) );
        }
    }

    static class IntersectReducer extends Reducer<Text, KeyValueWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<KeyValueWritable> values, Context context) throws IOException, InterruptedException {
            int result = 0;
            Set<String> filenames = new HashSet<String>();
            for (KeyValueWritable value : values) {
                result += value.getValue().get();
                filenames.add(value.getKey().toString());
            }
            if(filenames.size() == 2)
            	context.write(key, new IntWritable(result));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(IntersectHadoop.class);

        job.setMapperClass(LineMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(KeyValueWritable.class);

        job.setReducerClass(IntersectReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job, new Path("results/hadoop/wordsentencecount/part-r-00000"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("results/hadoop/wordcount/part-r-00001"), TextInputFormat.class);

        TextOutputFormat.setOutputPath(job, new Path("results/hadoop/intersectcount"));
        
        job.waitForCompletion(true);
    }

}
