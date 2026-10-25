package edu.cs6510.servicebased.checkout;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.*;
import edu.cs6510.servicebased.checkout.common.BusinessException;
import org.springframework.web.server.ResponseStatusException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.ArrayList;
import java.util.concurrent.Future;
import static org.junit.jupiter.api.Assertions.*;

/** 功能：使用真实临时 HTTP 服务验证商品查询合同和上游故障处理。 */
class CatalogClientTest {
 HttpServer server;
 CatalogClient client;
 AtomicInteger requests=new AtomicInteger();
 volatile int status=200;
 volatile String body="{\"items\":[{\"sku\":\"SKU-000001\",\"name\":\"Store Item 000001\",\"price\":1.01}]}";
 @BeforeEach void start() throws Exception {
  server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  server.createContext("/items",exchange->{
   requests.incrementAndGet();
   byte[] bytes=body.getBytes(StandardCharsets.UTF_8);
   exchange.getResponseHeaders().set("Content-Type","application/json");
   exchange.sendResponseHeaders(status,bytes.length);
   try(var stream=exchange.getResponseBody()) { stream.write(bytes); }
  });
  server.start();client=new CatalogClient("http://127.0.0.1:"+server.getAddress().getPort());
 }
 @AfterEach void stop() { if(client!=null) client.close(); if(server!=null) server.stop(0); }
 @Test void readsRemoteProductAndExactPrice() {
  var item=client.find("SKU-000001");
  assertEquals("Store Item 000001",item.name());assertEquals(new BigDecimal("1.01"),item.price());
 }
 @Test void unknownSkuHasBusiness404() {
  var ex=assertThrows(BusinessException.class,()->client.find("missing"));
  assertEquals(BusinessException.Kind.NOT_FOUND,ex.getKind());
 }
 @Test void upstreamFailureIs503() {
  status=500;
  assertEquals(503,assertThrows(ResponseStatusException.class,()->client.find("SKU-000001")).getStatusCode().value());
 }
 @Test void invalidPayloadIs503() {
  body="{\"items\":null}";
  assertEquals(503,assertThrows(ResponseStatusException.class,()->client.find("SKU-000001")).getStatusCode().value());
 }
 @Test void subsequentScansAndUnknownSkuUseLocalSnapshot() {
  client.find("SKU-000001");status=500;
  assertEquals(new BigDecimal("1.01"),client.find("SKU-000001").price());
  assertThrows(BusinessException.class,()->client.find("missing"));
  assertEquals(1,requests.get());
 }
 @Test void concurrentFirstScansLoadOnlyOnce() throws Exception {
  try(var executor=Executors.newVirtualThreadPerTaskExecutor()) {
   var futures=new ArrayList<Future<?>>();
   for(int n=0;n<40;n++) futures.add(executor.submit(()->assertEquals("SKU-000001",client.find("SKU-000001").sku())));
   for(var future:futures) future.get();
  }
  assertEquals(1,requests.get());
 }
 @Test void failedLoadDoesNotPoisonCache() {
  status=503;assertThrows(ResponseStatusException.class,()->client.find("SKU-000001"));
  status=200;client.find("SKU-000001");client.find("SKU-000001");
  assertEquals(2,requests.get());
 }
 @Test void emptyCatalogCanBeRetriedAfterInitialization() {
  String populated=body;body="{\"items\":[]}";
  assertThrows(BusinessException.class,()->client.find("SKU-000001"));
  body=populated;client.find("SKU-000001");assertEquals(2,requests.get());
 }
}
