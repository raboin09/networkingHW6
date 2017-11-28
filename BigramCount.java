
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class BigramCount extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(BigramCount.class);

    private BigramCount() {
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        
        //creates CLI options so you can see input commands and descriptions of the commands
        //Example CLI: path   input path
        
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        
        //create command line object and parse object to grab input, output, and number of reducer tasks from args and compare to options

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: "
                    + exp.getMessage());
            return -1;
        }

        //check if command line has both input and output options, if it doesn't then display args and exit
        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        //assign option values INPUT, OUTPUT, NUM_REDUCERS to strings 
        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
                .parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;
        //if reduceTasks has an input, assign it to reduceTasks, else assign it 1 by default
        
        //log the commands into logger
        LOG.info("Tool name: " + BigramCount.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - num reducers: " + reduceTasks);

        //create new job, retrieve configuration
        Job job = new Job(getConf());
        job.setJobName(BigramCount.class.getSimpleName());
        job.setJarByClass(BigramCount.class);

        //assign attributes of job (input, output, numReduceTasks)
        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //assign output key/value types
        job.setMapOutputKeyClass(BigramWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(BigramWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //assign mapper and reducer classes
        job.setMapperClass(BigramCountMapper.class);
        job.setReducerClass(BigramCountReducer.class);

        //wot
        Path outputDir = new Path(outputPath);
        FileSystem.get(getConf()).delete(outputDir, true);

        //track time taken to complete
        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BigramCount(), args); //creates CLI inputs instead of hard-coding the number of tasks, input, etc... runs run()
    }
}
 
