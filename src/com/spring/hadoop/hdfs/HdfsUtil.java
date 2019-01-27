package com.spring.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;
//
public class HdfsUtil {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();//读配置文件s
		FileSystem fs = FileSystem.get(conf);
		System.out.println();
		Path src = new Path("hdfs://spring:9000/jdk-7u80-linux-x64.tar.gz");
		FSDataInputStream open = fs.open(src);
		FileOutputStream fileOutputStream = new FileOutputStream("/Users/spring/Downloads/l/jdk-7u80-linux-x64.tar.gz");
		IOUtils.copy(open, fileOutputStream);
	}
	@Test
	public void s() throws Exception {
		FileInputStream in = new FileInputStream(new File("h.txt"));
		FileOutputStream out = new FileOutputStream(new File("uploadFile"));
		byte[] buff = new byte [1024];
		int length = -1;
	
		while((length = in.read(buff))!=-1) {
			String s = new String(buff,"GBK");
			byte[] bytes = s.getBytes("UTF8");
			out.write(bytes, 0, length);
		}
	}
	//底层上传文件
	@Test
	public void upload() throws IOException {
//		System.setProperty("HADOOP_USER_NAME","hadoop");
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path("hdfs://spring:9000/uploadFile");
		FSDataOutputStream out = fileSystem.create(path);
		FileInputStream in = new FileInputStream("uploadFile");
		IOUtils.copy(in, out);
	}
	// 封装后上传文件
	@Test
	public void upload2() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://spring:9000/"), conf, "hadoop");
		System.out.println("123");
		fileSystem.copyFromLocalFile(new Path("/Users/spring/Documents/eclipseWorkspace/Hadoop/uploadFile"), new Path("hdfs://spring:9000/aa/bb/uploadFile"));
	}
	// 下载文件
	@Test
	public void load() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://spring:9000/"), conf, "hadoop");
		fileSystem.copyToLocalFile(new Path("hdfs://spring:9000/uploadFile"), new Path("/Users/spring/Documents/eclipseWorkspace/Hadoop/uploadFile"));
	}
	// 创建文件夹
	@Test
	public void mkdir() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://spring:9000/"), conf, "hadoop");
		fileSystem.mkdirs(new Path("/aa/bb"));
	}
	
	// 删除
	@Test
	public void rm() throws IllegalArgumentException, IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://spring:9000/"), conf, "hadoop");
		fs.delete(new Path("/aa"),true);
		// 递归删除
	}
	// 查看文件信息
	@Test
	public void list() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://spring:9000/"), conf, "hadoop");
		//listFile 列出文件信息 提供递归查看
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()) {
			LocatedFileStatus file = listFiles.next();
			Path path = file.getPath();
			String name = path.getName();
			System.out.println(name);
		}
		System.out.println();
		
		//listStatus 列出指定目录的文件和文件夹信息 不支持递归
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus status : listStatus) {
			String name = status.getPath().getName();
			System.out.println(name + (status.isDirectory()?" is dir":" is file"));
		}
	}
}
