package com.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
// 使用Mapreduce包中的Mapper
// mapred代表的是hadoop旧API，而mapreduce代表的是hadoop新的API
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 毛福林
 *
 * @title: StudentScoreMapper
 * @projectName hadoop-training
 * @description: TODO
 * @date 2020/1/122:31
 */
public class StudentScoreMapper extends Mapper<Object,Text, Text, IntWritable> {

    private Text outKeyPass = new Text("Math_Score_Pass");
    private Text outKeyNotPass = new Text("Math_Score_Not_Pass");

    /**
     * 数据格式：200412169,gavin,male,30,0401,math=24&en=60&c=85&os=78,math=22&en=20&c=85&os=78
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 1. valueIn为文件中每行的数据,使用split方法进行分割
        String[] vals = value.toString().split(",");
        // 2.每行数据中，倒数第二列为第一学期成绩
        if(vals.length != 7 ){return;}
        String scoreStr = vals[5];
        // 3.每门成绩按照&分割
        String[] scores = scoreStr.split("&");
        if(scores.length != 4){return;}
        // 4.获取数学成绩,学科成绩按照=分割
        String mathScoreStr = scores[0];
        String[] mathScore = mathScoreStr.split("=");
        if(mathScore.length != 2){return;}
        String score = mathScore[1];
        // 5.判断成绩是否及格
        int temp = Integer.parseInt(score);
        if(temp >= 60){
            context.write(outKeyPass,new IntWritable(1));
        }else{
            context.write(outKeyNotPass,new IntWritable(1));
        }
    }

}
