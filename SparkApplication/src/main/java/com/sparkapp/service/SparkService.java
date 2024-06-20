package com.sparkapp.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import scala.collection.JavaConverters;
import scala.collection.Seq;



@Service
public class SparkService 
{
    @Value("${spring.data.mongodb.uri}")
    private String mongodbUri;
    
    @Value("${spring.data.mongodb.database}")
    private String mongodbname;
    
    private String destMongoUri = "mongodb+srv://admin:admin%40123@rajutestinstance.gksq4tf.mongodb.net/";
    private String destMongodbName = "MongoToMongo";
    	
    public ResponseEntity<?> migrateMongoToMongo()
    {
        long startTime = System.currentTimeMillis();
    	try(SparkSession session = SparkSession.builder().appName("Migrate Mongo to Mongo").master("local[*]").getOrCreate())
    	{
    		Dataset<Row> mongoData = session.read()
                    .format("mongo")
                    .option("uri", mongodbUri)
                    .option("database", mongodbname)
                    .option("collection", "parent_child_relation")
                    .load();
    		
    		mongoData.write().mode(SaveMode.Overwrite)
    		.format("mongo")
			.option("uri", destMongoUri)
			.option("database", destMongodbName)
			.option("collection", "joinedDF")
			.save();
            List<Map<String, Object>> result = mapDataToList(mongoData);
            session.stop();
    		logExecutionTime(startTime);
    		return new ResponseEntity<>(result, HttpStatus.OK);
    	}
        catch(Exception e)
    	{
        	e.printStackTrace();
        	return new ResponseEntity<>("Exception in Application", HttpStatus.BAD_REQUEST);
    	}
    }
    
    private void logExecutionTime(long startTime) 
    {
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime) / 1000; // Convert to seconds
        System.out.println("Execution time in seconds: " + duration);
    }
    
//    private List<Map<String, Object>> mapDataToList(Dataset<Row> mongoData) 
//    {
//        return mongoData.collectAsList().stream()
//                .map(row -> {
//                    Map<String, Object> map = new HashMap<>();
//                    for (String field : row.schema().fieldNames()) {
//                        map.put(field, row.getAs(field));
//                    }
//                    return map;
//                }).collect(Collectors.toList());
//    }
       

    private List<Map<String, Object>> mapDataToList(Dataset<Row> mongoData) 
    {
        return mongoData.collectAsList().stream()
                .map(this::rowToMap)
                .collect(Collectors.toList());
    }
    
    private Map<String, Object> rowToMap(Row row) 
    {
        Map<String, Object> map = new HashMap<>();
        for (String field : row.schema().fieldNames()) 
        {
            Object value = row.getAs(field);
            map.put(field, convertValue(value));
        }
        return map;
    }
    
    private Object convertValue(Object value) 
    {
        if (value instanceof Row) 
        {
            return rowToMap((Row) value);
        } 
        else if (value instanceof Seq) 
        {
            Seq<?> seq = (Seq<?>) value;
            List<Object> list = new ArrayList<>();
            for (Object elem : JavaConverters.seqAsJavaList(seq)) 
            {
                list.add(convertValue(elem));
            }
            return list;
        } 
        else 
        {
            return value;
        }
    }
    
    
}
