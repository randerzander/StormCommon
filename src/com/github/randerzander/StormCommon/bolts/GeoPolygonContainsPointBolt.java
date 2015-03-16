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
import org.apache.commons.lang3.ArrayUtils;

public class GeoPolygonContainsPointBolt implements IRichBolt {
  private HashMap<String, Polygon> polygons = new HashMap();
  private ArrayList<String> fields;
  private Fields outputFields;
  private String polygonID;
  private String coordinatesField;
  private String latitudeField;
  private String longitudeField;

  private OutputCollector collector;

  public GeoPolygonContainsPointBolt(){}

  public GeoPolygonContainsPointBolt withCoordinatesField(String coordinatesField){ this.coordinatesField = coordinatesField;  return this; }
  public GeoPolygonContainsPointBolt withLatitudeField(String latitudeField){ this.latitudeField = latitudeField; return this; }
  public GeoPolygonContainsPointBolt withLongitudeField(String longitudeField){ this.longitudeField = longitudeField; return this; }
  public GeoPolygonContainsPointBolt withPolgyonID(String polygonId){ this.polygonID = polygonID; return this; }
  public GeoPolygonContainsPointBolt withOutputFields(String[] fields){ this.outputFields = new Fields(fields); return this; }

  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){ this.collector = collector; }

  public void execute(Tuple tuple){
    //System.err.println("INTERSECTION: " + tuple.getSourceComponent() + ": ");
    //for (String field: tuple.getFields()) System.err.println(field + ": " + tuple.getStringByField(field));
    String coordinates = tuple.getStringByField(this.coordinatesField);
    String pointLat = tuple.getStringByField(this.latitudeField);
    String pointLng = tuple.getStringByField(this.longitudeField);
    Point point = new Point(Float.valueOf(pointLat), Float.valueOf(pointLng));

    String key = (this.polygonID != null) ? tuple.getStringByField(this.polygonID) : coordinates;
    Polygon polygon = polygons.get(key);
    if (polygon == null){
      polygon = loadPolygon(coordinates);
      polygons.put(key,polygon);
    }

    if (polygon.contains(point)) collector.emit(tuple,tuple.getValues());
    collector.ack(tuple);
  }

  private Polygon loadPolygon(String coordinates){
    Builder builder = Polygon.Builder();
    for (String point : coordinates.split(" - ")){
      String[] tokens = point.split(",");
      builder.addVertex(new Point(Float.valueOf(tokens[0]), Float.valueOf(tokens[1])));
    }

    return builder.build();
  }

@Override
  public Map<String, Object> getComponentConfiguration(){ return null; }
  @Override
  public void cleanup(){}
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer){ declarer.declare(this.outputFields); }
}
