package com.swsandbox;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * User: jgreenwald
 * Date: 7/6/13
 * Time: 11:22 PM
 */
public class HelloWorldJob extends Configured implements Tool
{
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new HelloWorldJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] strings) throws Exception
    {
        Job job = new Job(getConf(), "HelloWorldJob");
        job.setJarByClass(getClass());

        job.setMapperClass(HelloWorldMapper.class);
        job.setReducerClass(HelloWorldReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //output section
        job.setOutputFormatClass(CqlOutputFormat.class);
        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "hadoop_sample", "messages");
        String query = "update hadoop_sample.messages set msg = ?";
        CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
        ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
        ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");

        //input section
        job.setInputFormatClass(CqlPagingInputFormat.class);
        ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), "hadoop_sample", "input");
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");

//        CqlConfigHelper.setInputWhereClauses(job.getConfiguration(), "name = 'Jesse' ");


        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}
