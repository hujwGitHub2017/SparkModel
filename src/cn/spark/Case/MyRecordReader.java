package cn.spark.Case;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class MyRecordReader extends RecordReader<Text, Text>{
	
	private FSDataInputStream input = null;
	
	private Text Keys = null;
	
	private Text Values = null;
	
	private long fileLength  = 0;
	
	private long length  = 0;

	private BufferedReader in = null;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
			FileSplit fileSplit = (FileSplit)split;
		
			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			
			input = fileSystem.open(fileSplit.getPath());
			
			Keys = new Text(fileSplit.getPath().getName());
			
			
//			Keys = new Text(String.valueOf(fileSplit.getLength()));
			
			// fileIn.seek(start);
//			in = new BufferedInputStream(new GZIPInputStream(fileIn));
			
//			in = new BufferedInputStream(input);
			
			in = new BufferedReader(new InputStreamReader(input));
			
			fileLength = fileSplit.getLength();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		boolean flag = false;
		
		String line = null;
		
		if (Values == null) {
			
			Values = new Text();
			
		}
		
		/*if (Keys == null) {
			
			Keys = new Text();
		}*/
		
		if ((line = in.readLine()) != null){
			
			Values.set(line);
			
//			Keys.set(String.valueOf(line.length()));
			
			flag = true;
			
			length += line.length();
		}
			
		
		
		return flag;
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		
		return Keys;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		
		return Values;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		
		
		return length/fileLength;
		
	}

	@Override
	public void close() throws IOException {
		
		if (in != null) {
			
			in.close();
		}
		
		if (input != null) {
			
			input.close();
		}
		
		
	}

}
