package com.sparkapp.controller;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sparkapp.service.SparkService;


@RestController
public class SparkController 
{
	@Autowired
    private SparkService sparkService;
	
	
	@PostMapping("/migratesqltomongo")
	public ResponseEntity<?> migrateSqlToMongo() 
	{
		return sparkService.migrateSqlToMongo();
	}
	
	@PostMapping("/migratemongotosql")
	public ResponseEntity<?> migrateMongoToSql() 	
	{
		return sparkService.migrateMongoToSql();
	}
	
	@PostMapping("/migratemongotomongo")
	public ResponseEntity<?> migrateMongoToMongo()
	{
		return sparkService.migrateMongoToMongo();
	}

	@PostMapping("/processcsvtomongo")
	public ResponseEntity<?> sparkThroughCsvToMongo() 
	{
		try 
		{
			String filePath = copyResourceToTempFile("annual-csv.csv");
			return sparkService.processCsvToMongo(filePath);
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			return new ResponseEntity<>("File Reading Error: " + e.getMessage(), HttpStatus.BAD_REQUEST);
		}
	}

	@PostMapping("/processcsvfilestomongo")
	public ResponseEntity<?> sparkThroughCsvFilesToMongo() 
	{
		try 
		{
			String filePath = copyResourceToTempFile("annual-csv.csv");
			String filePath2 = copyResourceToTempFile("annual-csv-join.csv");
			return sparkService.processCsvToMongo(filePath, filePath2);
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			return new ResponseEntity<>("File Reading Error: " + e.getMessage(), HttpStatus.BAD_REQUEST);
		}
	}
	
	@PostMapping("/processcsvtosql")
	public ResponseEntity<?> sparkThroughCsvToMySql() 
	{
		try 
		{
			String filePath = copyResourceToTempFile("annual-csv.csv");
			return sparkService.processCsvToMySql(filePath);
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			return new ResponseEntity<>("File Reading Error: " + e.getMessage(), HttpStatus.BAD_REQUEST);
		}
	}
	
	@PostMapping("/processcsvfilestosql")
	public ResponseEntity<?> sparkThroughCsvFilesToMySql() 
	{
		try 
		{
			String filePath = copyResourceToTempFile("annual-csv.csv");
			String filePath2 = copyResourceToTempFile("annual-csv-join.csv");
			return sparkService.processCsvToMySql(filePath, filePath2);
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			return new ResponseEntity<>("File Reading Error: " + e.getMessage(), HttpStatus.BAD_REQUEST);
		}
	}
	
	@PostMapping("/migratemongotocassandra")
	public ResponseEntity<?> sparkThroughMongoToCassandra() 
	{
		return sparkService.migrateMongoToCassandra();
	}
	
	@PostMapping("/migratecassandratomongo")
	public ResponseEntity<?> sparkThroughCassandraToMongo() 
	{
		return sparkService.migrateCassandraToMongo();
	}
	
	@PostMapping("/modifydatasql")
	public ResponseEntity<?> modifyDataSql() 
	{
		return sparkService.modifyDataInMySql();
	}
	
	@PostMapping("/modifycolsql")
	public ResponseEntity<?> modifyColumnSql() 
	{
		return sparkService.renameColumnInMySql();
	}
	
	@PostMapping("/modifyTomongo")
	public ResponseEntity<?> modifyDataMongo() 
	{
		return sparkService.modifyDataInMongo();

	}
	
	@PostMapping("/db2db")
	public ResponseEntity<?> migrateSqlToSql() 
	{
		return sparkService.migrateSqlToSql();
	}
	
	private String copyResourceToTempFile(String resourcePath) throws IOException 
	{
        Resource resource = new ClassPathResource(resourcePath);
        InputStream inputStream = resource.getInputStream();
        File tempFile = File.createTempFile(resourcePath, ".csv");

        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }
        
        return tempFile.getAbsolutePath();
    }
}
