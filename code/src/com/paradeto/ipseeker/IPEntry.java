package com.paradeto.ipseeker;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/** *
* 一条IP范围记录，不仅包括国家和区域，也包括起始IP和结束IP * 

 * 
 * @author swallow */
public class IPEntry {
    public String beginIp;
    public String endIp;
    public String country;
    public String area;
    
    /**
     * 构造函数
     */
   
 

 public IPEntry() {
        beginIp = endIp = country = area = "";
    }
    
    public String toString(){
       return this.area+"  "+this.country+"IP范围:"+this.beginIp+"-"+this.endIp;
    }
   } 

