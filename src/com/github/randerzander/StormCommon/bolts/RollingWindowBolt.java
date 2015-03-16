package com.github.randerzander.StormCommon.bolts;

import com.github.randerzander.StormCommon.Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.DateTime;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.Config;

import org.apache.phoenix.jdbc.PhoenixDriver;

public class RollingWindowBolt implements IRichBolt {
  private String jdbcURL;
  private Connection connection;
  private OutputCollector collector;
  private Fields outputFields;
  private String source;
  private String[] sourceFields;
  private String table;
  private ArrayList<ArrayList<Object>> window;
  private String[] types;
  private DateTimeFormatter formatter;
  private String timestampField;
  private int timestampFieldIndex = -1;
  private int duration;
  private int maxSlots;

  public RollingWindowBolt(String jdbcURL){ this.jdbcURL = jdbcURL; }
  public RollingWindowBolt withOutputFields(String[] fields){ this.outputFields = new Fields(fields); return this; }
  public RollingWindowBolt withSource(String source, String table){ this.source = source; this.table = table; return this; }
  public RollingWindowBolt withSourceFields(String[] fields){ this.sourceFields = fields; return this; }
  public RollingWindowBolt withCountRotation(int maxSlots){ this.maxSlots = maxSlots; return this; }
  public RollingWindowBolt withTimeRotation(String format, String timestampField, int duration){
    this.formatter = DateTimeFormat.forPattern(format);
    this.timestampField = timestampField;
    this.duration = duration;
    return this;
  }
  
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
    this.collector = collector;
    this.window = new ArrayList<ArrayList<Object>>();
    try{
      connection = DriverManager.getConnection(jdbcURL, new Properties());
      ResultSet rows = connection.createStatement().executeQuery("select * from " + this.table);
      this.types = Utils.getTypes(rows);
      while (rows.next()){ this.window.add(Utils.ArrayListFromRow(rows, this.types.length)); };
      connection.close();
    }catch(Exception e){ e.printStackTrace(); throw new RuntimeException(e); }
  }

  public boolean evict(ArrayList<Object> row, Tuple tuple){
    if (this.timestampField != null){
      DateTime now = this.formatter.parseDateTime(tuple.getStringByField(this.timestampField));
      DateTime then = this.formatter.parseDateTime((String) row.get(this.timestampFieldIndex));
      if (then.isBefore(now.minusSeconds(this.duration))) return true;
      else return false;
    }else return this.window.size() > this.maxSlots;
  }

  @Override
  public void execute(Tuple tuple){
    //Handle incoming window tuples
    if (this.timestampFieldIndex == -1 && this.timestampField != null) this.timestampFieldIndex = tuple.fieldIndex(this.timestampField);
    if (tuple.getSourceComponent().equals(this.source)){ //If this tuple is a window addition
      for (Iterator<ArrayList<Object>> iterator = this.window.iterator(); iterator.hasNext();)
        if (evict(iterator.next(), tuple)) iterator.remove();
      this.window.add(new ArrayList<Object>(tuple.getValues()));
    }
    //Handle incoming events from other streams
    else{
      for (Iterator<ArrayList<Object>> iterator = this.window.iterator(); iterator.hasNext();){
        collector.emit(tuple, new Values(Collections.addAll(tuple.getValues(), iterator.next())));
      }
    }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer){ declarer.declare(this.outputFields); }
  @Override
  public void cleanup(){}
  @Override
  public Map<String, Object> getComponentConfiguration() { return null; }
}
