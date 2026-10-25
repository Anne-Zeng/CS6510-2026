package edu.cs6510.servicebased.checkout;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
/** 功能：事件投递与结账恢复使用独立线程，Analytics 停机不阻塞库存协调恢复。 */
@Configuration
public class SchedulingConfig {
 @Bean public ThreadPoolTaskScheduler taskScheduler() { return scheduler("checkout-recovery-"); }
 @Bean public ThreadPoolTaskScheduler outboxScheduler() { return scheduler("checkout-outbox-"); }
 private ThreadPoolTaskScheduler scheduler(String prefix) {
  var scheduler=new ThreadPoolTaskScheduler();scheduler.setPoolSize(1);scheduler.setThreadNamePrefix(prefix);return scheduler;
 }
}
