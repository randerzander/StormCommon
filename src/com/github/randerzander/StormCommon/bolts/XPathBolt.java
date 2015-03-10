package com.github.randerzander.StormCommon.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.xml.sax.InputSource;
import org.w3c.dom.Document;
import javax.xml.xpath.XPath;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;

import java.util.AbstractMap;
import java.util.HashMap;
import java.io.StringReader;

import java.util.Map;

public class XPathBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Fields outputFields;
    private String expression;
    private DocumentBuilderFactory dbf;
    private DocumentBuilder db;
    private XPathFactory xpf;
    private XPath xpath;

    public XPathBolt withOutputFields(String[] fields){ this.outputFields = new Fields(fields); return this; }

    public XPathBolt withExpression(String expression){ this.expression = expression; return this; }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
      dbf = DocumentBuilderFactory.newInstance();
      try{
        db = dbf.newDocumentBuilder();
        xpf = XPathFactory.newInstance();
        xpath = xpf.newXPath();
      }catch(Exception e){ e.printStackTrace(); throw new RuntimeException(); }
    }

    @Override
    public void execute(Tuple tuple) {
      Values outputTuple = new Values();
      try{
        Document document = db.parse(new InputSource(new StringReader(tuple.getString(0))));
        for (String field: outputFields) outputTuple.add(xpath.evaluate(this.expression+field, document));
        this.collector.emit(tuple, outputTuple);
      }catch(Exception e){ e.printStackTrace(); }
      this.collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(outputFields); }
}
