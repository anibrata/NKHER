package edu.umd.nkher;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cern.colt.Arrays;

public class BooleanRetrievalCompressed extends Configured implements Tool {

  // class variables
  private MapFile.Reader mapReader;
  private FSDataInputStream collection;
  private Stack<Set<Integer>> answerStack;

  public BooleanRetrievalCompressed() {
  };

  private void initialize(String indexPath, String collectionPath, FileSystem fs)
      throws IllegalArgumentException, IOException {
    mapReader =
        new MapFile.Reader(new Path(indexPath + "/part-r-00000"), fs.getConf());
    collection = fs.open(new Path(collectionPath));
    answerStack = new Stack<Set<Integer>>();
  }

  public void runQuery(String s) throws IOException {
    String a[] = s.split("\\s+");
    for (String s1 : a) {
      if (s1.equals("AND")) {
        performAND();
      }
 else if (s1.equals("OR")) {
        performOR();
      } else {
        pushTerm(s1);
      }
    }
    Set<Integer> answerSet = answerStack.pop();
    for (Integer i : answerSet) {
      String line = fetchLine(i);
        if (line.length() >= 80) {
            line = line.substring(0, 80);
        }
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    answerStack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> set1 = answerStack.pop();
    Set<Integer> set2 = answerStack.pop();
    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : set1) {
      if (set2.contains(n)) {
        sn.add(n);
      }
    }
    answerStack.push(sn);
  }

  private void performOR() {
    Set<Integer> set1 = answerStack.pop();
    Set<Integer> set2 = answerStack.pop();
    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : set1) {
      sn.add(n);
    }

    for (int n : set2) {
      sn.add(n);
    }
    answerStack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Text textkey = new Text();
    textkey.set(term);
    Set<Integer> set1 = new TreeSet<Integer>();
    BytesWritable postings = new BytesWritable();
    postings = fetchPostings(term);
    ByteArrayInputStream byteInputStream = new ByteArrayInputStream(postings.copyBytes());
    DataInputStream dataInputStream = new DataInputStream(byteInputStream);
    int counter = 0;
    int previousDocNumber = 0;
    while (dataInputStream.available() > 0) {
      if (counter == 0) {
        int docFreq = WritableUtils.readVInt(dataInputStream); // skipping document frequency
        counter++;
      }
      int docno = WritableUtils.readVInt(dataInputStream);
      int termfreq = WritableUtils.readVInt(dataInputStream);
      set1.add(docno + previousDocNumber);
      previousDocNumber += docno;
    }
    byteInputStream.close();
    dataInputStream.close();
    return set1;
  }

  private BytesWritable fetchPostings(String term) throws IOException {
    Text key = new Text();
    BytesWritable value = new BytesWritable();
    key.set(term);
    mapReader.get(key, value);
    return value;
  }

  private String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader bReader = new BufferedReader(new InputStreamReader(collection));
    return bReader.readLine();
  }

  private static final String INDEX = "index";
  private static final String COLLECTION = "collection";

  @Override
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INDEX));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(COLLECTION));

    org.apache.commons.cli.CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INDEX) || !cmdline.hasOption(COLLECTION)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(LookupPostings.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String indexPath = cmdline.getOptionValue(INDEX);
    String collectionPath = cmdline.getOptionValue(COLLECTION);

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(indexPath, collectionPath, fs);

    String[] queries =
        { "outrageous fortune AND", "white rose AND", "means deceit AND",
            "white red OR rose AND pluck AND",
            "unhappy outrageous OR good your AND OR fortune AND" };

    for (String query : queries) {
      System.out.println("Query is : " + query);
      runQuery(query);
      System.out.println("");
    }

    return 0;
  }

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
