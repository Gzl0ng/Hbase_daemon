package com.gzl0ng.mr1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 郭正龙
 * @date 2021-11-26
 */
public class FruitMapper extends Mapper<LongWritable, Text,LongWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
