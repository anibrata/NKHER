package edu.umd.nkher;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class GetPostings extends Configured implements Tool {
	
	Logger LOG = Logger.getLogger(GetPostings.class);
	
	private static final String INDEX = "index";
	private static final String WORD = "word";
	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("index").hasArg()
				.withDescription("HBase table name").create(INDEX));
		options.addOption(OptionBuilder.withArgName("word").hasArg()
				.withDescription("word to look up").create(WORD));
		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			return -1;
		}
		if (!cmdline.hasOption(INDEX) || !cmdline.hasOption(WORD)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}
		
		String tableName = cmdline.getOptionValue(INDEX);
		String word = cmdline.getOptionValue(WORD);

		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		Configuration hbaseConfig = HBaseConfiguration.create(conf);
		HConnection hbaseConnection = HConnectionManager.createConnection(hbaseConfig);
		HTableInterface table = hbaseConnection.getTable(tableName);
		Get get = new Get(Bytes.toBytes(word));
		Result result = table.get(get);		
		
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			System.out.println("Doc number : " + Bytes.toInt(cell.getQualifier()) + " TF : " + Bytes.toInt(cell.getValue()));
		}
	
		return 1;
	}
	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GetPostings(), args);
	}
}

