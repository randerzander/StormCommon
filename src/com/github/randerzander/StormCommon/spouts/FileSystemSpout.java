package com.github.randerzander.StormCommon.spouts;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public abstract class FileSystemSpout extends BaseRichSpout  {
	private SpoutOutputCollector collector;
	public abstract List<File> listFiles();
	private Set<File> workingSet = new HashSet<File>();
	private int interval;

  public FileSystemSpout withInterval(int interval){ this.interval = interval; return this; }

	@Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
  }
	
	@Override
	public void nextTuple() {
		try {
      //Check for new files
			for (File file : listFiles()) {
				if (!workingSet.contains(file)) {
					collector.emit(new Values(file.getAbsolutePath()), file.getAbsoluteFile());
					workingSet.add(file);
				}
			}
			Thread.sleep(this.interval);
		} catch (Exception e) { e.printStackTrace(); throw new RuntimeException(e); }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields("path")); }
	
	@Override
	public void fail(Object msgId) { workingSet.remove(new File((String)msgId)); }
}
