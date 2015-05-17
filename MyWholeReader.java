import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class MyWholeReader extends RecordReader<NullWritable, BytesWritable>{
	
	private FileSplit filesplit;
	private Configuration conf;
	private boolean processed=false;
	private BytesWritable value=new BytesWritable();
	@Override
	public void initialize(InputSplit split,TaskAttemptContext context)
	{
		this.filesplit=(FileSplit)split;
		this.conf=context.getConfiguration();
	}
	@Override
	public boolean nextKeyValue() throws IOException,InterruptedException
	{
		if(!processed)
		{
		byte[] myfilecontent=new byte[(int) filesplit.getLength()];
		Path file=filesplit.getPath();
		FileSystem fs=file.getFileSystem(conf);
		FSDataInputStream in=null;
		try
		{
	    in=fs.open(file);
		IOUtils.readFully(in,myfilecontent,0,myfilecontent.length);
		value.set(myfilecontent,0,myfilecontent.length);
		}
		finally
		{
			IOUtils.closeStream(in);
		}
		processed=true;
		return true;
		}
		return false;
	}
		public NullWritable getCurrentKey() throws IOException,InterruptedException
		{
			return NullWritable.get();
		}
		public BytesWritable getCurrentValue() throws IOException
		{
			return value;
		}
		public void close() throws IOException
		{
			//do nothing
		}
		public float getProgress() throws IOException {
			return processed ? 1.0f : 0.0f;
			}
		

}
