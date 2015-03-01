package edu.umd.nkher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Stack;
import java.util.Set;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;
import cern.colt.Arrays;

public class BooleanRetrieval extends Configured implements Tool{
  
  // class variables
  private MapFile.Reader mapReader;
  private FSDataInputStream collection;
  private Stack<Set<Integer>> answerStack;

  public BooleanRetrieval() {
  };
  
  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IllegalArgumentException, IOException {
    mapReader =
        new MapFile.Reader(new Path(indexPath + "/part-r-00000"),
            fs.getConf());
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
    Set<Integer> set = new TreeSet<Integer>();
    ArrayListWritable<PairOfInts> list = new ArrayListWritable<PairOfInts>();
    list = fetchPostings(term);
    for (PairOfInts pair : list) {
      set.add(pair.getLeftElement());
    }
    return set;
  }

  private ArrayListWritable<PairOfInts> fetchPostings(String term)
      throws IOException {
    Text key = new Text();
    PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value =
        new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();
    key.set(term);
    mapReader.get(key, value);
    return value.getRightElement();
  }

  private String fetchLine(long i) throws IOException {
    collection.seek(i);
    BufferedReader bReader =
        new BufferedReader(new InputStreamReader(collection));
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

    if (collectionPath.endsWith(".gz")) {
      System.out
          .println("gzipped collection is not seekable: use compressed version!");
      System.exit(-1);
    }

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

    return 1;
  }
  
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrieval(), args);
  }

}
