package com.example.controller;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.example.DocumentSimilarityMapper;
import com.example.DocumentSimilarityReducer;

public class DocumentSimilarityDriver {
    
    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.err.println("Usage: DocumentSimilarityDriver <inputPath1> <inputPath2> <outputPath>");
            System.exit(1);
        }
    
        JobConf conf = new JobConf(DocumentSimilarityDriver.class);
        conf.setJobName("WordCount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(DocumentSimilarityMapper.class);
        // conf.setCombinerClass(DocumentSimilarityReducer.class);
        conf.setReducerClass(DocumentSimilarityReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
    
        FileInputFormat.setInputPaths(conf, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
    
        JobClient.runJob(conf);
    }
    
}
