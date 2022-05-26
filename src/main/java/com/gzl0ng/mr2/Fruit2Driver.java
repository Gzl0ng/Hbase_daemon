package com.gzl0ng.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author 郭正龙
 * @date 2021-11-29
 */
public class Fruit2Driver implements Tool {

    //定义配置信息
    private Configuration conf = null;


    @Override
    public int run(String[] strings) throws Exception {

        //1.获取job对象
        Job job = Job.getInstance();

        //2.设置主类路径
        job.setJarByClass(Fruit2Driver.class);

        //3.设置Mapper&输出KV类型
        TableMapReduceUtil.initTableMapperJob("fruit",
                new Scan(),
                Fruit2Mapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);

        //4.设置Reducer&输出的表
        TableMapReduceUtil.initTableReducerJob("fruit2",
                Fruit2Reducer.class,
                job);

        //5.提交任务
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) {

        try {
//            Configuration configuration = new Configuration();
            Configuration configuration = HBaseConfiguration.create();
            ToolRunner.run(configuration,new Fruit2Driver(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
