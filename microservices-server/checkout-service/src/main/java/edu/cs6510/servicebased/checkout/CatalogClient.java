package edu.cs6510.servicebased.checkout;

import edu.cs6510.servicebased.checkout.contract.*;
import edu.cs6510.servicebased.checkout.common.BusinessException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.server.ResponseStatusException;
import jakarta.annotation.PreDestroy;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
import static edu.cs6510.servicebased.checkout.common.BusinessException.Kind.NOT_FOUND;

/** 功能：沿用上一版不可变目录缓存；数据仅通过 Catalog API 加载，不跨服务访问数据库。
 * 本作业目录初始化后固定；商品变更需要重启 Catalog 和 Checkout 刷新快照。
 */
@Component
public class CatalogClient {
 private final HttpClient http;
 private final RestClient client;
 private volatile Map<String,CatalogItemResponse> bySku=Map.of();
 public CatalogClient(@Value("${catalog.base-url:http://localhost:8081}") String url) {
  http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).version(HttpClient.Version.HTTP_1_1).build();
  var factory=new JdkClientHttpRequestFactory(http);factory.setReadTimeout(Duration.ofSeconds(3));
  client=RestClient.builder().baseUrl(url).requestFactory(factory).build();
 }
 public CatalogItemResponse find(String sku) {
  var snapshot=bySku;
  if(snapshot.isEmpty()) snapshot=load();
  var item=snapshot.get(sku);
  if(item==null) throw new BusinessException(NOT_FOUND,"SKU_NOT_FOUND","SKU not found");
  return item;
 }
 /** 功能：并发首次扫描只发起一次加载；失败或空目录不缓存，后续请求可以重试。 */
 private synchronized Map<String,CatalogItemResponse> load() {
  if(!bySku.isEmpty()) return bySku;
  try {
   var response=client.get().uri("/items").retrieve().body(CatalogResponse.class);
   if(response==null || response.items()==null) throw new IllegalArgumentException("Invalid Catalog response");
   for(var item:response.items()) {
    if(item==null || item.sku()==null || item.sku().isBlank() || item.sku().length()>32 || item.name()==null || item.name().isBlank() || item.name().length()>255 || item.price()==null || item.price().signum()<0)
     throw new IllegalArgumentException("Invalid Catalog item");
   }
   var loaded=response.items().stream().collect(Collectors.toUnmodifiableMap(CatalogItemResponse::sku,item->item));
   bySku=loaded;return loaded;
  } catch(RestClientException | IllegalArgumentException | IllegalStateException ex) {
   throw new ResponseStatusException(SERVICE_UNAVAILABLE,"Catalog service unavailable",ex);
  }
 }
 @PreDestroy public void close() { http.close(); }
}
