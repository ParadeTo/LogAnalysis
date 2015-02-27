package com.paradeto.ana;
import com.paradeto.pro.Process;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.fs.Path;
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
 * 统计访问浏览器
 * @author youxingzhi
 *
 */
public class Explorer {
	private static long bytesperday = 0;
	private static int ipsperday = 0;
    public static class KPIExplorerMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
        private Text browser = new Text();
        private Text ips = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	String line = value.toString();
        	Process kpi = Process.parser(line);
        	//filter spider
            if (kpi.isValid()&&!kpi.isSpider()) {
                ips.set(kpi.getRemote_addr());
                browser.set(kpi.getBrowser());
                output.collect(browser, ips);
            }
        }
    }

    public static class KPIExplorerReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	long bytesperip = 0;
        	Set<String> ips = new HashSet<String>();
        	while (values.hasNext()) {
                ips.add(values.next().toString());
            }
        	String outvalue = String.valueOf(ips.size());
            output.collect(key, new Text(outvalue));
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://master:9000/logdata/";
        String output = "hdfs://master:9000/explorer";

        JobConf conf = new JobConf(Explorer.class);
        conf.setJobName("Explorer");
        //conf.addResource("classpath:/hadoop/core-site.xml");
        //conf.addResource("classpath:/hadoop/hdfs-site.xml");
        //conf.addResource("classpath:/hadoop/mapred-site.xml");
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setMapperClass(KPIExplorerMapper.class);
        //conf.setCombinerClass(KPIExplorerReducer.class);
        conf.setReducerClass(KPIExplorerReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
//        System.out.println("Request filter spiders:");
//        System.out.println("IPs per day:");
//        System.out.println(ipsperday);
//        System.out.println("Bytes per day:");
//        System.out.println(bytesperday);
        System.exit(0);
    }

}

