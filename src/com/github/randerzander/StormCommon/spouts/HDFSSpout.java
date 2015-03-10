package com.github.randerzander.StormCommon.spouts;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import com.google.common.collect.Lists;

public class HDFSSpout extends FileSystemSpout {
  private File dir;

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    Object path = conf.get("path");
    if (path != null) this.dir = new File((String) path);
  }
  
  @Override
  public List<File> listFiles() {
    List<File> files = Lists.newLinkedList();
    Configuration conf = new Configuration();
    Path inputDirPath = new Path(this.dir.getAbsolutePath());
    FileSystem fs = null;
    try {
      fs = DistributedFileSystem.get(URI.create(this.dir.getAbsolutePath()), conf);
      for (FileStatus status : fs.listStatus(inputDirPath)) {
        if (status.isDir()) continue;
        files.add(new File(status.getPath().toUri().getPath()));
      }
    }catch (IOException e) { e.printStackTrace(); }
    finally {
      if (fs != null) {
        try { fs.close(); }
        catch (IOException e) { e.printStackTrace(); }
      }
    }
    return files;
  }
}
