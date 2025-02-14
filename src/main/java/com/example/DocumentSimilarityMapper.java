package com.example;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * Mapper class for calculating document similarity.
 * This class reads words from input documents and emits (word, filename) pairs.
 */
public class DocumentSimilarityMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private Text wordKey = new Text();      // Stores the word as the key
    private Text fileNameValue = new Text(); // Stores the filename as the value

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        // Identify the input file name from the input split
        FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        fileNameValue.set(fileName); // Set the file name as the value

        // Tokenize the input text line into words
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        while (tokenizer.hasMoreTokens()) {
            // Extract next word and clean it (remove punctuation, convert to lowercase)
            String word = tokenizer.nextToken().replaceAll("[^a-zA-Z0-9]", "").toLowerCase();

            // Ensure the word is not empty after cleaning
            if (!word.isEmpty()) {
                wordKey.set(word); // Set the word as the key
                output.collect(wordKey, fileNameValue); // Emit the (word, filename) pair
            }
        }
    }
}


