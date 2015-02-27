package com.paradeto.ana;
import com.paradeto.pro.Process;
import com.paradeto.ipseeker.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
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
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Address {

	public static int ipnum=0;

    public static class KPIAddressMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        //private Text word = new Text();
        private IntWritable one = new IntWritable(1);
        private Text ips = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        	String line = value.toString();
        	//find image
        	Process kpi = Process.parser(line);
            if (kpi.isValid()&&!kpi.isSpider()) {
                //word.set(kpi.getRequest());
                ips.set(kpi.getRemote_addr());
                //System.out.println(ips.toString()+"\t"+one.toString());
                output.collect(ips, one);
            }
        }
    }

    public static class KPIAddressReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();
        private Set<String> count = new HashSet<String>();
    	IPSeeker ipseeker = IPSeeker.getInstance();
        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //ip to address

        	
        	String address = ipseeker.getAddress(key.toString());
        	//System.out.println(key.toString()+"\t"+address);
        	//System.out.println("hello");
        	int sum=0;
        	if (values.hasNext()) {
        		
        		sum = 1;
                //count.add(values.next().toString());
            }
        	String outvalue = Integer.toString(sum)+"+"+address;
            //result.set(String.valueOf(count.size()));
            output.collect(key, new Text(outvalue));
        }
    }
    public static class KPIAddressMapper2 extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        //private Text word = new Text();

    	
       // private IntWritable one = new IntWritable(1);
       // private Text ips = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        	String line = value.toString();
        	StringTokenizer tokenizer = new StringTokenizer(line);
        	String str="";
        	//System.out.println(line);
        	while(tokenizer.hasMoreTokens()){
        		String tmp = tokenizer.nextToken();
        		//System.out.println(tmp);
        		if(tmp.contains("+")){
        			str = tmp;
        			break;
        		}
        	}
        	int sep = str.indexOf("+");
        	//ystem.out.println()
        	int num = Integer.parseInt(str.substring(0,sep));
        	String address = str.substring(sep+1);
        	ipnum = ipnum+num;
        	//System.out.println(address+"\t"+Integer.toString(num));
            output.collect(new Text(address), new IntWritable(num));

        }
    }
    public static class KPIAddressReducer2 extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();
        private Set<String> count = new HashSet<String>();
    	IPSeeker ipseeker = IPSeeker.getInstance();
        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //ip to address

        	
        	//String address = ipseeker.getAddress(key.toString());
        	//System.out.println(key.toString()+"\t"+address);
        	//System.out.println("hello");
        	int sum=0;
        	while (values.hasNext()) {
        		sum = sum+values.next().get();
                //count.add(values.next().toString());
            }
        	float percent = (float)sum/(float)ipnum*100;
        	String tmp = String.valueOf(percent)+"%";
        	String outvalue = Integer.toString(sum)+"\t"+Integer.toString(ipnum)+"\t"+tmp;
        	//System.out.println(key.toString()+"\t"+outvalue);
            output.collect(key, new Text(outvalue));
        }
    }
    
    public static void main(String[] args) throws Exception {
        String input = "hdfs://master:9000/logdata";
        String output = "hdfs://master:9000/addressadv";

		Path tempDir = new Path("hdfs://master:9000/AddressTemp");

        JobConf conf = new JobConf(Address.class);
        conf.setJobName("KPIAddress");
        //conf.addResource("classpath:/hadoop/core-site.xml");
        //conf.addResource("classpath:/hadoop/hdfs-site.xml");
        //conf.addResource("classpath:/hadoop/mapred-site.xml");
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setMapperClass(KPIAddressMapper.class);
        //conf.setCombinerClass(null);
        conf.setReducerClass(KPIAddressReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, (tempDir));

        JobClient.runJob(conf);
        
        JobConf conf2 = new JobConf(Address.class);
        conf2.setJobName("KPIAddress2");
        //conf.addResource("classpath:/hadoop/core-site.xml");
        //conf.addResource("classpath:/hadoop/hdfs-site.xml");
        //conf.addResource("classpath:/hadoop/mapred-site.xml");
        
        conf2.setMapOutputKeyClass(Text.class);
        conf2.setMapOutputValueClass(IntWritable.class);
        
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);
        
        conf2.setMapperClass(KPIAddressMapper2.class);
        //conf.setCombinerClass(null);
        conf2.setReducerClass(KPIAddressReducer2.class);

        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf2, tempDir);
        FileOutputFormat.setOutputPath(conf2, new Path(output));

        JobClient.runJob(conf2);
        System.exit(0);
    }

}

