package cn.spark.Case;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

public class MyMultipleTextOutputFormat<k,v> extends MyMultipleOutputFormat<k,v>{
	
	private TextOutputFormat<k,v> textoutput = null;

	@Override
	protected RecordWriter<k,v> getBaseRecordWriter(FileSystem fs,
			JobConf job, String name, Progressable arg3) throws IOException {
		
		if(textoutput == null){
			
			
			textoutput = new TextOutputFormat<k, v>();
			
		}
		
			//		System.out.println("aa--"+arg3);
		
		return textoutput.getRecordWriter(fs, job, name, arg3) ;
	}

}
