package com.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 毛福林
 * @title: StudentScoreReducer
 * @projectName hadoop-training
 * @description: TODO
 * @date 2020/1/421:07
 */
public class StudentScoreReducer extends Reducer<Text, IntWritable,Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.经过shuffle之后 相同key的结果value 组成一个Iterable，作为reduce的输入参数进行计算
        //2.遍历Iterable，将期所有的val进行累加
        long sum = 0L;
        for(IntWritable val : values){
            sum += val.get();
        }
        //3.返回计算结果
        context.write(key,new LongWritable(sum));
    }
}
