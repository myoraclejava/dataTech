package com.linjinwu;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Hello world!
 *
 */
@SpringBootApplication
public class App {
    public static void main( String[] args ) {
        System.out.println(args);
        ConfigurableApplicationContext run = SpringApplication.run(App.class, args);
    }
}
