package com.github.randerzander.StormCommon.bolts;

import com.github.randerzander.StormCommon.Utils;

import org.apache.storm.hdfs.bolt.AbstractHdfsBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;

public class TimeSyncedHdfsBolt extends AbstractHdfsBolt{
    private static final Logger LOG = LoggerFactory.getLogger(TimeSyncedHdfsBolt.class);

    private transient FSDataOutputStream out;
    private RecordFormat format;
    private long offset = 0;
    private int frequency = 60;

    public TimeSyncedHdfsBolt withFsUrl(String fsUrl){
        this.fsUrl = fsUrl;
        return this;
    }

    public TimeSyncedHdfsBolt withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }

    public TimeSyncedHdfsBolt withFileNameFormat(FileNameFormat fileNameFormat){
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public TimeSyncedHdfsBolt withRecordFormat(RecordFormat format){
        this.format = format;
        return this;
    }

    public TimeSyncedHdfsBolt withSyncPolicy(int seconds){ this.frequency = seconds; return this; }

    public TimeSyncedHdfsBolt withRotationPolicy(FileRotationPolicy rotationPolicy){
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public TimeSyncedHdfsBolt addRotationAction(RotationAction action){
        this.rotationActions.add(action);
        return this;
    }

    @Override
    void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) throws IOException {
        LOG.info("Preparing HDFS Bolt...");
        this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
    }

    @Override
    public void execute(Tuple tuple) {
        if (Utils.isTickTuple(tuple)){
          if (this.out instanceof HdfsDataOutputStream) {
              ((HdfsDataOutputStream) this.out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
          } else {
              this.out.hsync();
          }
        }else{
            try {
              byte[] bytes = this.format.format(tuple);
              synchronized (this.writeLock) {
                  out.write(bytes);
                  this.offset += bytes.length;
              }

              this.collector.ack(tuple);

              if(this.rotationPolicy.mark(tuple, this.offset)){
                  rotateOutputFile(); // synchronized
                  this.offset = 0;
                  this.rotationPolicy.reset();
              }
          } catch (IOException e) {
              LOG.warn("write/sync failed.", e);
              this.collector.fail(tuple);
          }
        }
    }

    void closeOutputFile() throws IOException {
        this.out.close();
    }

    Path createOutputFile() throws IOException {
        Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
        this.out = this.fs.create(path);
        return path;
    }

    public Map<String, Object> getComponentConfiguration(){
      Config conf = new Config();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, this.frequency);
      return conf;
    }
}
