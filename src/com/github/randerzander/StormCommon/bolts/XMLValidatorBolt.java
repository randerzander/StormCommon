package com.github.randerzander.StormCommon.bolts;

import java.util.ArrayList;
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

public class XMLValidatorBolt implements IRichBolt {
  private ArrayList<String> fields;
  private String XSD;
  private String XMLFieldName;
  private SchemaFactory factory;
  private Schema schema;
  private Validator validator;
  private OutputCollector collector;

  public XMLValidatorBolt(){}

  public XMLValidatorBolt withXSD(String XSD){ this.XSD = XSD;  return this; }
  public XMLValidatorBolt withXMLFieldName(String XMLFieldName){ this.XMLFieldName = XMLFieldName; return this; }

  public XMLValidatorBolt withFields(String[] fields){ 
   this.fields = new ArrayList<String>();
   for(String field: fields) this.fields.add(field);
   this.fields.add("valid");
   return this;
  }

  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
    this.collector = collector;
    factory =  SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
  }

  private boolean validate(String XML){
    try{ this.validator.validate(new StreamSource(new StringReader(XML))); }
    catch(Exception e){ return false; }
    return true;
  }

  public void execute(Tuple tuple){
    if (schema == null){
      try{
        this.schema = this.factory.newSchema(new StreamSource(new StringReader(this.XSD)));
        this.validator = this.schema.newValidator();
      }catch(Exception e){ e.printStackTrace(); throw new RuntimeException(e); }
    }

    List<Object> output = new ArrayList<Object>(tuple.getValues()); 
    output.add(validate(tuple.getStringByField(this.XMLFieldName)));
    collector.emit(output);
  }

  @Override
  public Map<String, Object> getComponentConfiguration(){ return null; }
  @Override
  public void cleanup(){}
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer){ declarer.declare(new Fields(this.fields)); }
}
