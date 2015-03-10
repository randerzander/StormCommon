package com.github.randerzander.StormCommon.bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.Serializable;
import java.io.StringReader;

import javax.xml.XMLConstants;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;
import javax.xml.transform.stream.StreamSource;

import com.sromku.polygon.Point;
import com.sromku.polygon.Polygon;
import com.sromku.polygon.Polygon.Builder;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class GenericFilterBolt<A,B> implements IRichBolt {
	
	public interface IFilterComparator<A,B> extends Serializable{
		public abstract boolean compare(A fieldCompareFrom, B fieldCompareTo);
		public abstract A getCompareFrom(Tuple tuple, String fieldName);
		public abstract B getCompareTo(Tuple tuple, String fieldName);
		
	}
	
	public static class FilterComparatorEquals implements IFilterComparator<String, String>{
		
		public FilterComparatorEquals(){
			super();
		};
		public boolean compare(String field1, String field2) {
			return field1.equals(field2);
		}

		public String getCompareFrom(Tuple tuple, String fieldName) {
			return tuple.getStringByField(fieldName);
		}

		public String getCompareTo(Tuple tuple, String fieldName) {
			return tuple.getStringByField(fieldName);
		}
	}

	private IFilterComparator comparator;
	private ArrayList<String> fields;
	private String fieldCompareFrom = "field1";
	private String fieldCompareTo = "field2";

	private OutputCollector collector;

	public GenericFilterBolt(){}

	public GenericFilterBolt<A,B> withOutputFields(String[] fields){ 
		this.fields = new ArrayList<String>();
		for(String field: fields) this.fields.add(field);
		return this;
	}
	
	public GenericFilterBolt<A,B> withComparator(IFilterComparator<A,B> comparator){ 
		this.comparator = comparator;
		return this;
	}
	
	public GenericFilterBolt<A,B> withFieldCompareFrom(String fieldCompareFrom){ 
		this.fieldCompareFrom = fieldCompareFrom;
		return this;
	}
	
	public GenericFilterBolt<A,B> withFieldCompareTo(String fieldCompareTo){ 
		this.fieldCompareTo = fieldCompareTo;
		return this;
	}
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){ this.collector = collector; }

	public void execute(Tuple tuple){
		if (this.comparator.compare(this.comparator.getCompareFrom(tuple, this.fieldCompareFrom), this.comparator.getCompareTo(tuple, this.fieldCompareTo))){
			collector.emit(tuple,tuple.getValues());
		}
		
		collector.ack(tuple);
	}

	public Map<String, Object> getComponentConfiguration(){ return null; }
	public void cleanup(){}
	public void declareOutputFields(OutputFieldsDeclarer declarer){ declarer.declare(new Fields(this.fields)); }
	
}
