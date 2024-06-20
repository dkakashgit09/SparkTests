package com.sparkapp.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sparkapp.service.SparkService;


@RestController
public class SparkController 
{
	@Autowired
    private SparkService sparkService;
	
	@PostMapping("/migratemongotomongo")
	public ResponseEntity<?> migrateMongoToMongo()
	{
		return sparkService.migrateMongoToMongo();
	}
}
