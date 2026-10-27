package edu.cs6510.events;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class MessagingConfiguration {
    @Bean(destroyMethod = "close")
    public MessageBus messageBus(@Value("${spring.application.name}") String name) {
        return new KafkaBus(name.replace("-service", ""));
    }
}
