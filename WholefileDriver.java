import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class WholefileDriver {
	public static void main(String args[]) throws Exception
	{
		if(args.length!=1)
			{
			System.err.println("Usage: Worddrivernewapi <input path> <output path>");
			System.exit(-1);
			}
		Job job=new Job();
		job.setJarByClass(WholefileDriver.class);
		job.setJobName("WholefileDriver");
		
		FileInputFormat.setInputPaths(job,"test/test_file.dat,test/sequence_test.dat,test/paircount.dat,test/file.txt");
		FileOutputFormat.setOutputPath(job,new Path(args[0]));
		
		job.setMapperClass(WholefileMapper.class);
		
		job.setInputFormatClass(Wholefileinputformat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
