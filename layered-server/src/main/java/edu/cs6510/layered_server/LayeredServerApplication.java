package edu.cs6510.layered_server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Scheduling drives the analytics layer's window publisher off the request path. */
@SpringBootApplication
@EnableScheduling
public class LayeredServerApplication {

	public static void main(String[] args) {
		SpringApplication.run(LayeredServerApplication.class, args);
	}

}
