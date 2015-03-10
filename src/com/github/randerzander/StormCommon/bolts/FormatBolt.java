package com.github.randerzander.StormCommon.bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.XMLConstants;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

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

public class FormatBolt implements IRichBolt {

	public interface IFormat extends Serializable {
		public String format(String rootElement, List<String> fields, Tuple tuple) throws Exception;
	}
	
	public static class FormatXML implements IFormat{

		public String format(String rootElementName, List<String> fields, Tuple tuple) throws Exception {
	
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			TransformerFactory tf = TransformerFactory.newInstance();			
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();;
			Transformer transformer = tf.newTransformer();

			// root element
			Document doc = docBuilder.newDocument();
			Element rootElement = doc.createElement(rootElementName);
			doc.appendChild(rootElement);
	 
			// elements
			for(String field: fields){
				Element element = doc.createElement(field);
				element.appendChild(doc.createTextNode(tuple.getStringByField(field)));
				rootElement.appendChild(element);	
			}

			DOMSource domSource = new DOMSource(doc);
			StringWriter writer = new StringWriter();
			StreamResult result = new StreamResult(writer);

			transformer.transform(domSource, result);
			return writer.toString();
			
		}
		
	}
	private ArrayList<String> fields;
	private String rootElement = "root";
	private IFormat format;

	private OutputCollector collector;

	public FormatBolt(){}

	public FormatBolt withRootElement(String rootElement){ 
		this.rootElement = rootElement;
		return this;
	}
	
	public FormatBolt withOutputFields(String[] fields){ 
		this.fields = new ArrayList<String>();
		for(String field: fields) this.fields.add(field);
		return this;
	}
	
	public FormatBolt withFormat(IFormat format){ 
		this.format = format;
		return this;
	}
	public FormatBolt withFormat(String format){ 
    if (format.equalsIgnoreCase("XML")) this.format = new FormatXML();
    else throw new RuntimeException(format + " is an unrecognized format");
		return this;
	}
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){ this.collector = collector; }

	public void execute(Tuple tuple)  {
		List newTuple = new ArrayList<String>();
		try {
			newTuple.add(format.format(rootElement, fields, tuple));
		} catch (Exception e) {
			e.printStackTrace();
		}
		collector.emit(tuple,newTuple);
		collector.ack(tuple);
	}


	public Map<String, Object> getComponentConfiguration(){ return null; }
	public void cleanup(){}
	public void declareOutputFields(OutputFieldsDeclarer declarer){ 
		declarer.declare(new Fields("xml"));

	}
}
