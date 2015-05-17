import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WholefileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{
	private Text filenamekey;
	public void map(NullWritable key,BytesWritable value,Context context) throws IOException,InterruptedException
	{
		context.write(filenamekey,value);
	}
	
	public void setup(Context context) throws IOException
	{
		InputSplit split=context.getInputSplit();
		Path path=((FileSplit)split).getPath();
		filenamekey=new Text(path.toString());
	}
}
