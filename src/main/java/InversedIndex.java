import model.LineWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InversedIndex extends Configured implements Tool {

    public static final String processedPath = "/processed";
    public static final String tempPath = "/temp";

    @Override
    public int run(String[] args) throws Exception {

        JobControl jobControl = new JobControl("JobChain");

        String inputFilePath = args[0];
        String outputFilePath = args[1];

        // configure job1:
        Configuration conf1 = getConf();
        Job job1 = Job.getInstance(conf1);
        job1.setJobName("LineJob");
        job1.setJarByClass(LineJob.class);

        FileInputFormat.setInputPaths(job1, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job1, new Path(outputFilePath + tempPath));

        job1.setMapperClass(LineJob.OffsetMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LineWrapper.class);
        job1.setReducerClass(LineJob.OffsetReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        ControlledJob controlledJob1 = new ControlledJob(conf1);

        // configure job2
        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2, "WordIndexerJob");
        job2.setJarByClass(WordIndexerJob.class);

        job2.setMapperClass(WordIndexerJob.InversedIndexMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(WordIndexerJob.InversedIndexReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path(outputFilePath + tempPath + "/part*"));
        FileOutputFormat.setOutputPath(job2, new Path(outputFilePath + processedPath));

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.addDependingJob(controlledJob1);

        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        // run job1
        int result1 = job1.waitForCompletion(true) ? 0 : 1;

        // run job2
        if (result1 == 0) {
            return job2.waitForCompletion(true) ? 0 : 1;
        } else {
            return result1;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new InversedIndex(), args);
        System.exit(exitCode);
    }
}
