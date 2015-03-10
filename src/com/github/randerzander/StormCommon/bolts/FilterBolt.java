package com.github.randerzander.StormCommon.bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.StringReader;

import javax.xml.XMLConstants;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;
import javax.xml.transform.stream.StreamSource;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterBolt implements IRichBolt {

	public enum Condition{
		EQUALS() {
			@Override public boolean compare(String field1, String field2) {
				return field1.equals(field2);
			}
		},
		NOTEQUALS() {
			@Override public boolean compare(String field1, String field2) {
				return !field1.equals(field2);
			}
		},
		CONTAINS() {
			@Override public boolean compare(String field1, String field2) {
				return field1.contains(field2);
			};
		},
		NOTCONTAINS() {
			@Override public boolean compare(String field1, String field2) {
				return !field1.contains(field2);
			};
		};

		public abstract boolean compare(String field1, String field2);

	}

	private Condition condition;
	private ArrayList<String> fields;
	private String fieldCompareFrom;
	private String fieldCompareTo;

	private OutputCollector collector;

	public FilterBolt(){}

	public FilterBolt withOutputFields(String[] fields){ 
		this.fields = new ArrayList<String>();
		for(String field: fields) this.fields.add(field);
		return this;
	}

	public FilterBolt withCondition(Condition condition){ 
		this.condition = condition;
		return this;
	}
	
	public FilterBolt withFieldCompareFrom(String fieldCompareFrom){ 
		this.fieldCompareFrom = fieldCompareFrom;
		return this;
	}
	
	public FilterBolt withFieldCompareTo(String fieldCompareTo){ 
		this.fieldCompareTo = fieldCompareTo;
		return this;
	}
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){ this.collector = collector; }

	public void execute(Tuple tuple){
		if (this.condition.compare(tuple.getStringByField(this.fieldCompareFrom), tuple.getStringByField(this.fieldCompareTo))){
			collector.emit(tuple,tuple.getValues());
		}
		collector.ack(tuple);
	}

	public Map<String, Object> getComponentConfiguration(){ return null; }
	public void cleanup(){}
	public void declareOutputFields(OutputFieldsDeclarer declarer){ declarer.declare(new Fields(this.fields)); }
}
