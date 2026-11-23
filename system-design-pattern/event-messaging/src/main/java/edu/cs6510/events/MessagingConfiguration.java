package edu.cs6510.events;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
//为spring服务创建并注入MessageBus，Spring 创建其他业务对象(CatalogClient、InventoryClient)时，把已创建的 MessageBus 传给它们
@Configuration(proxyBeanMethods = false)
public class MessagingConfiguration {
    //destroyMethod: Spring 容器正常关闭时，调用这个对象的 close() 方法，释放 Kafka 连接、消费者和相关线程等资源
    @Bean(destroyMethod = "close")//bean调用下面的方法，把返回的对象放进 Spring 容器，由 Spring 管理
    public MessageBus messageBus(@Value("${spring.application.name}") String name) {
        //根据配置创建通信对象: 创建连接 Kafka 的客户端通信对象
        return new KafkaBus(name.replace("-service", ""));
    }
}
