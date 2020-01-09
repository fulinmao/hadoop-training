package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 毛福林
 * @title: StudentScoreMain
 * @projectName hadoop-training
 * @description: TODO
 * @date 2020/1/421:23
 */
public class StudentScoreMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置任务的名称
        job.setJobName("StudentScoreCount");
        //设置任务执行的主类
        job.setJarByClass(StudentScoreMain.class);
        //设置reduce任务的个数
        job.setNumReduceTasks(1);
        //设置Map类
        job.setMapperClass(StudentScoreMapper.class);
//        //设置combiner，在Map端执行reduce任务
//        job.setCombinerClass(StudentScoreReducer.class);
        //设置Reduce类
        job.setReducerClass(StudentScoreReducer.class);
        //设置输出key的类
        job.setOutputKeyClass(Text.class);
        //设置输出value的类
        job.setOutputValueClass(LongWritable.class);
        //如果Map和Reduce的输出类型不一致，需要单独对map设置输出key和value的类型
        //同时注释setCombinerClass方法
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置Map输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //设置Reduce输处路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
