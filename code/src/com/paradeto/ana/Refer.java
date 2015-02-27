package com.paradeto.ana;
import com.paradeto.pro.Process;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * 统计访问来源
 * @author youxingzhi
 *
 */
public class Refer { 

    public static class KPIReferMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
        

        @Override
        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	Text ip = new Text();
            Text word = new Text();
        	String line = value.toString();
        	Process kpi = Process.parser(line);
            if (kpi.isValid()&&!kpi.isSpider()) {
                word.set(kpi.getHttp_referer_domain());
                ip.set(kpi.getRemote_addr());
                output.collect(word, ip);
            }
        }
    }

    public static class KPIReferReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        //private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            Set<String> ipset = new HashSet<String>();
        	int ipnum = 0;
            while (values.hasNext()) {
                ipset.add(values.next().toString());
            }
            ipnum = ipset.size();
            
            output.collect(key, new Text(String.valueOf(ipnum)));
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://master:9000/logdata";
        String output = "hdfs://master:9000/pv";

        JobConf conf = new JobConf(Refer.class);
        conf.setJobName("Refer");
        //conf.addResoCopyOfKPIReferurce("classpath:/hadoop/core-site.xml");
        //conf.addResource("classpath:/hadoop/hdfs-site.xml");
        //conf.addResource("classpath:/hadoop/mapred-site.xml");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(KPIReferMapper.class);
        //conf.setCombinerClass(KPIReferReducer.class);
        conf.setReducerClass(KPIReferReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);
    }

}

