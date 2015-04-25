package com.github.randerzander.StormCommon.bolts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PhoenixLookupBolt implements IRichBolt {
  private String jdbcURL;
  private Connection connection;
  private OutputCollector collector;
  private PreparedStatement statement;
  private String query;
  private String[] fieldMap;

  public PhoenixLookupBolt(String jdbcURL){ this.jdbcURL=jdbcURL; }

  public PhoenixLookupBolt withPreparedStatement(String query, String[] statementParamToFieldMap){
    this.query = query;
    this.fieldMap = statementParamToFieldMap;
    return this;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
    this.collector = collector;
    try{ 
      connection = DriverManager.getConnection(jdbcURL, new Properties());
      statement = connection.prepareStatement(this.query);
    }catch(Exception e){ e.printStackTrace(); throw new RuntimeException(e); }
    
  }

  @Override
  public void execute(Tuple tuple){
    int column = 1;
    for (String field: this.fieldMap){
      Object value = tuple.getValueByField(field);
      try{
        if (value instanceof String) statement.setString(column++, tuple.getStringByField(field));
        else if (value instanceof Integer) statement.setInt(column++, tuple.getIntegerByField(field));
        else if (value instanceof Long) statement.setLong(column++, tuple.getLongByField(field));
        else if (value instanceof Double) statement.setDouble(column++, tuple.getDoubleByField(field));
        else if (value instanceof Float) statement.setFloat(column++, tuple.getFloatByField(field));
      }catch(Exception e){ e.printStackTrace(); throw new RuntimeException(e); }
    }

    List<Object> output = new ArrayList<Object>(tuple.getValues()); 
    try{
      ResultSet results = statement.executeQuery();
      ResultSetMetaData meta = results.getMetaData();
      int columns = meta.getColumnCount();

      while (results.next()){
        for (int i = 1; i <= columns; i++) output.add(results.getObject(i));
      }
    }catch(Exception e){ e.printStackTrace(); throw new RuntimeException(e); }

    collector.emit(output);
    collector.ack(tuple);
  }

  @Override
  public Map<String, Object> getComponentConfiguration(){ return null; }
  @Override
  public void cleanup(){}
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer){}
}
