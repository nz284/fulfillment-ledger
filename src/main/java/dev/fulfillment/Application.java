package dev.fulfillment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class Application {
    public static void main(String[] args) {
        var context = SpringApplication.run(Application.class, args);
        if (context.getEnvironment().matchesProfiles("dlt-replay")) context.close();
    }
}
