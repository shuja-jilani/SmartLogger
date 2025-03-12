package com.parseresdb.parseresdb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ParseresdbApplication {

	public static void main(String[] args) {
		SpringApplication.run(ParseresdbApplication.class, args);
	}

}
