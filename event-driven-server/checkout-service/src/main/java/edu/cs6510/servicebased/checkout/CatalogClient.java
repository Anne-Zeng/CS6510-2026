package edu.cs6510.servicebased.checkout;

import edu.cs6510.events.Json;
import edu.cs6510.events.MessageBus;
import edu.cs6510.servicebased.checkout.contract.*;
import edu.cs6510.servicebased.checkout.common.BusinessException;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import java.util.Map;
import java.util.stream.Collectors;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
import static edu.cs6510.servicebased.checkout.common.BusinessException.Kind.NOT_FOUND;

/** Catalog owns the data; Checkout loads an immutable snapshot through Kafka.
 * The assignment catalog is fixed after initialization. Restart to refresh it.
 */
@Component
public class CatalogClient {
 private final MessageBus bus;
 private volatile Map<String,CatalogItemResponse> bySku=Map.of();
 public CatalogClient(MessageBus bus) { this.bus=bus; }
 public CatalogItemResponse find(String sku) {
  var snapshot=bySku;
  if(snapshot.isEmpty()) snapshot=load();
  var item=snapshot.get(sku);
  if(item==null) throw new BusinessException(NOT_FOUND,"SKU_NOT_FOUND","SKU not found");
  return item;
 }
 /** Only the first concurrent scan loads; failures and empty catalogs can retry. */
 private synchronized Map<String,CatalogItemResponse> load() {
  if(!bySku.isEmpty()) return bySku;
  try {
   var reply=bus.request("catalog","GET_CATALOG","catalog","{}");
   if(reply.status()!=200) throw new IllegalStateException("Catalog returned "+reply.status());
   var response=Json.read(reply.body(),CatalogResponse.class);
   if(response==null || response.items()==null) throw new IllegalArgumentException("Invalid Catalog response");
   for(var item:response.items()) {
    if(item==null || item.sku()==null || item.sku().isBlank() || item.sku().length()>32 || item.name()==null || item.name().isBlank() || item.name().length()>255 || item.price()==null || item.price().signum()<0)
     throw new IllegalArgumentException("Invalid Catalog item");
   }
   var loaded=response.items().stream().collect(Collectors.toUnmodifiableMap(CatalogItemResponse::sku,item->item));
   bySku=loaded;return loaded;
  } catch(RuntimeException ex) {
   throw new ResponseStatusException(SERVICE_UNAVAILABLE,"Catalog service unavailable",ex);
  }
 }
}
