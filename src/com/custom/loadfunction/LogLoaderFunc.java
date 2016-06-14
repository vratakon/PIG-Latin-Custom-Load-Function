package com.custom.loadfunction;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class LogLoaderFunc extends LoadFunc {
	 
	@SuppressWarnings("rawtypes")
	private RecordReader reader;
    private TupleFactory tupleFactory;
 
   
	@SuppressWarnings("rawtypes")
	@Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }
    
      
    @Override
    public Tuple getNext() throws IOException {
        Tuple tuple = null;
        String value2=null;
        tupleFactory = TupleFactory.getInstance();
        try {
            boolean notDone = reader.nextKeyValue();
            if (!notDone) {
                return null;
            }
            Text value = (Text) reader.getCurrentValue();

            if(value != null) {
            String value1 = value.toString().replaceAll("\"", "");
            String[] cut =value1.split(" \\("); 
            int splitat=cut[0].length();
        	String[] words=cut[0].toString().split(" ");
        	String cut1=value1.toString().substring(splitat);
         	String output="";
         	for(int i=0;i<words.length;i++)
         	{
         		output=output+words[i]+',';
         	}
         	String output1=output+cut1;
         	value2=output1.replaceAll("\\[","").replaceAll("\\]","").toString();
            

                        tuple = tupleFactory.newTuple(value2);
            }
            
           } catch (InterruptedException e) {
            e.printStackTrace();
           }

           return tuple;

          }
    @SuppressWarnings("rawtypes")
	@Override
    public void prepareToRead(RecordReader reader, PigSplit pigSplit)
            throws IOException {
        this.reader = reader;
    }
    
    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }
 }