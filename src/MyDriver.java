/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author asn2
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {

    public static void main(String[] args) throws IOException, InterruptedException,
            ClassNotFoundException {
        //if (args.length < 2) {
        //    throw new IllegalArgumentException("args: inputpath outputpath");
        //}

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCounter");

        job.setJarByClass(MyDriver.class);
        
        job.setMapperClass(MyMapper.class);
    
        job.setCombinerClass(MyReducer.class);
        
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new  Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
}