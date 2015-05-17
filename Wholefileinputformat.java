

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class Wholefileinputformat extends FileInputFormat<NullWritable,BytesWritable>{

	public RecordReader<NullWritable,BytesWritable> createRecordReader(InputSplit split,TaskAttemptContext context)
	{
	return new MyWholeReader();
	}
	public boolean issplitable()
	{
		return false;
	}
}
