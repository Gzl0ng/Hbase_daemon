package com.gzl0ng.contants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @author 郭正龙
 * @date 2021-11-30
 */
public class Constants {

    //HBase的配置信息
    public static Configuration CONFIGURATION;
    static {
        CONFIGURATION = HBaseConfiguration.create();
        CONFIGURATION.set("hbase.zookeeper.quorum","node1,node2,node3");
    }

    //命名空间
    public static final String NAMESPACE = "weibo";

    //微博内容表
    public static final String CONTENT_TABLE= "weibo:content";
    public static final String CONTENT_TABLE_CF = "info";
    public static final int CONTENT_TABLE_VERSIONS = 1;

    //用户关系表
    public static final String RELATION_TABLE = "weibo:relation";
    public static final String RELATION_TABLE_CF1 = "attends";
    public static final String RELATION_TABLE_CF2 = "fans";
    public static final int RELATION_TABLE_VERSIONS = 1;

    //收件箱表
    public static final String INBOX_TABLE = "weibo:inbox";
    public static final String INBOX_TABLE_CF = "info";
    public static final int INBOX_TABLE_VERSIONS = 2;

}
