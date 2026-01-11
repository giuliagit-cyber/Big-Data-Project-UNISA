/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package progetto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 *
 * @author giuli
 */

public class DriverBMW {

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        
        Path inputPath;
        Path outputDir;
        Path outputDirStep2;
        int numberOfReducers;
        int k;
        // Parsing of the parameters
        numberOfReducers = Integer.parseInt(args[0]);
        inputPath = new Path(args[1]);
        outputDir = new Path(args[2]);
        outputDirStep2 = new Path(args[3]);
        k = Integer.parseInt(args[4]);

        Configuration conf = new Configuration();
        
        Job job1 = Job.getInstance(conf);
        
        job1.setJobName("Somma di Sales_Volume per Modello");
        
        // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
        FileInputFormat.addInputPath(job1, inputPath);

        // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job1, outputDir);

        // Specify the class of the Driver for this job
        job1.setJarByClass(DriverBMW.class);

        // Set job input format
        job1.setInputFormatClass(TextInputFormat.class);

        // Set job output format
        job1.setOutputFormatClass(TextOutputFormat.class);

        // Set map class
        job1.setMapperClass(Mapper1BMW.class);

        // Set map output key and value classes
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        // Set reduce class
        job1.setReducerClass(Reducer1BMW.class);
        job1.setCombinerClass(Reducer1BMW.class);

        // Set reduce output key and value classes
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // Set number of reducers
        job1.setNumReduceTasks(numberOfReducers);
        
        if(!job1.waitForCompletion(true)){
        
            System.exit(1);
        }
        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator","\t");
        
        conf2.setInt("top.k", k);

        Job job2 = Job.getInstance(conf2);
        
        job2.setJobName("Modello con massimo Sales_Volume");
        
        // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
        FileInputFormat.addInputPath(job2, outputDir);

        // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job2, outputDirStep2);

        // Specify the class of the Driver for this job
        job2.setJarByClass(DriverBMW.class);

        // Set job input format
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        // Set job output format
        job2.setOutputFormatClass(TextOutputFormat.class);

        // Set map class
        job2.setMapperClass(Mapper2BMW.class);

        // Set map output key and value classes
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(ModelSalesVolume.class);

        // Set reduce class
        job2.setReducerClass(Reducer2BMW.class);

        // Set reduce output key and value classes
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // Set number of reducers
        job2.setNumReduceTasks(1);
        
        // Execute the job and wait for completion
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
                
    }
    
}
