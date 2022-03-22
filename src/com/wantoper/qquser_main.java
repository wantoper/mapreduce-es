package com.wantoper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.http.HttpHost;
import org.apache.lucene.geo.Tessellator;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;

public class qquser_main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("es.nodes", "http://42.192.85.119");
        conf.set("'es.port","9200");
        conf.set("es.resource", "qquser/_doc");
        conf.set("es.input.json", "yes");
        conf.set("es.http.timeout","120m");
        conf.setBoolean("es.nodes.wan.only",true);

        Job job = Job.getInstance(conf,"QQUser");
        job.setJarByClass(qquser_main.class);

        job.setNumReduceTasks(0);

        job.setMapperClass(Mymap.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,"hdfs://master:9000/user/hive/warehouse/mydb.db/qq_user_phone_t/8e.txt");

        job.setOutputFormatClass(EsOutputFormat.class);
        job.waitForCompletion(true);
    }
}


class Mymap extends Mapper<LongWritable,Text, NullWritable,Text> {

    private static Text text = new Text();
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        String[] ss = value.toString().split("-");
        String mysjon = "{\"qq\":\" "+ss[0]+"\",\"tel\":\""+ss[1]+"\"}";
        text.set(mysjon);
        context.write(NullWritable.get(),text);
    }
}
