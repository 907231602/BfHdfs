package com.bf.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Test {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/**
		 * 代表的hadoop配置文件
		 */
		//上传
		Configuration conf=new Configuration();
		
		//hadoop文件系统
		try {
			FileSystem fileSystem=FileSystem.get(new URI("hdfs://yanjijun1:9000/"), conf);
			//上传一个文件
			FSDataOutputStream out= fileSystem.create(new Path("hdfs://yanjijun1:9000/bmd.txt"));
			FileInputStream in =new FileInputStream("D:/hadoop/trade_info.txt");
			//org.apache.hadoop.hdfs.server.namenode.NameNode
			IOUtils.copyBytes(in, out, conf);
			fileSystem.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//下载
		/*try {
			FileSystem fileSystem=FileSystem.get(new URI("hdfs://yanjijun1:9000/"), conf);
			
			FSDataInputStream input= fileSystem.open(new Path("hdfs://yanjijun1:9000/aa.txt"));
			FileOutputStream out=new FileOutputStream("D:/bmd.txt");
			IOUtils.copyBytes(input, out, conf);
			fileSystem.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		//列出文件列表
		/*try {
			FileSystem fileSystem=FileSystem.get(new URI("hdfs://yanjijun1:9000/"), conf);
			FileStatus[] status=fileSystem.listStatus(new Path("hdfs://yanjijun1:9000/"));
			
			for(FileStatus fs:status){
				System.out.println(fs.getPath().toString());
			}
			fileSystem.close();
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} */
		
		//删除文件
		/*try {
			FileSystem fileSystem=FileSystem.get(new URI("hdfs://yanjijun1:9000/"), conf);
			
			fileSystem.delete(new Path("hdfs://yanjijun1:9000/wc"));
			fileSystem.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} */
		
		//追加，自己修改目录
		/*try {
			FileSystem fileSystem=FileSystem.get(new URI("hdfs://yanjijun1:9000/"), conf);
			conf.set("dfs.client.block.write.replace-datanode-on-failure.policy",
                "NEVER"); 
			conf.set("dfs.client.block.write.replace-datanode-on-failure.enable",
                "true");
		
		FSDataOutputStream  dout=fileSystem.append(new Path("hdfs://yanjijun1:9000/aa.txt"));
		IOUtils.copyBytes( new FileInputStream("d:/bmd.txt"),dout,conf);
		fileSystem.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} */
		
		
	}

}
