package com.example.demo;

import javafx.application.Application;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IPSApplication {

	public static void main(String[] args) {
//		SpringApplication.run(IPSApplication.class, args);
		Application.launch(JavaFxApplication.class, args);
	}

}
