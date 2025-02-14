package com.example;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/**
 * Reducer class for calculating Jaccard similarity between two documents.
 * It processes words emitted by the mapper and determines the common and unique words.
 */
public class DocumentSimilarityReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private HashSet<String> uniqueWords = new HashSet<>(); // Stores all unique words (A ∪ B)
    private HashSet<String> commonWords = new HashSet<>(); // Stores common words (A ∩ B)
    private String fileName1 = "", fileName2 = ""; // Stores the two document names

    private OutputCollector outputCollector;
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        this.outputCollector=output;        
        HashSet<String> documentSet = new HashSet<>(); // Stores the document names for the current word

        // Collect document names for the current word
        while (values.hasNext()) {
            documentSet.add(values.next().toString());
        }

        // Track the two document names
        if (fileName1.isEmpty() && !documentSet.isEmpty()) {
            fileName1 = documentSet.iterator().next();
        } 
        if (fileName2.isEmpty() && documentSet.size() > 1) {
            for (String doc : documentSet) {
                if (!doc.equals(fileName1)) {
                    fileName2 = doc;
                    break;
                }
            }
        }

        // Add the word to the unique words set (A ∪ B)
        uniqueWords.add(key.toString());

        // If the word appears in both documents, add it to common words (A ∩ B)
        if (documentSet.size() == 2) {
            commonWords.add(key.toString());
        }
    }

    @Override
    public void close() throws IOException {
        // Compute Jaccard Similarity = |A ∩ B| / |A ∪ B|
        double jaccardSimilarity = uniqueWords.isEmpty() ? 0.0 : (double) commonWords.size() / uniqueWords.size();

        // Output document names being compared
        outputCollector.collect(new Text("Comparing Documents:"), new Text(fileName1 + " and " + fileName2));

        // Output the list of common words
        outputCollector.collect(new Text("Common Words (" + commonWords.size() + "):"), new Text(String.join(", ", commonWords)));

        // Output the total unique words across both documents
        outputCollector.collect(new Text("Unique Words (A ∪ B) (" + uniqueWords.size() + "):"), new Text(String.join(", ", uniqueWords)));

        // Output the Jaccard Similarity score
        outputCollector.collect(new Text("Jaccard Similarity:"), new Text(String.format("%.2f", jaccardSimilarity)));
    }
}




