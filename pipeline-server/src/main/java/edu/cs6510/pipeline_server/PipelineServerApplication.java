package edu.cs6510.pipeline_server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Starts checkout and the application-ready analytics pipeline. */
@SpringBootApplication
public class PipelineServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(PipelineServerApplication.class, args);
    }
}
