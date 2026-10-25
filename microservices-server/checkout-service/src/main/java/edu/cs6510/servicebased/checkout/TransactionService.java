package edu.cs6510.servicebased.checkout;
import edu.cs6510.servicebased.checkout.common.BusinessException;
import edu.cs6510.servicebased.checkout.contract.*;
import edu.cs6510.servicebased.checkout.persistence.CheckoutStore;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import static edu.cs6510.servicebased.checkout.common.BusinessException.Kind.INVALID_REQUEST;
import java.time.Instant;
import java.math.BigDecimal;
import java.util.UUID;

/** 功能：远程商品查询在本地数据库事务之外执行，避免网络等待占用事务连接。 */
@Service
public class TransactionService {
 private final CheckoutStore checkout;
 private final CatalogClient catalog;
 private final TransactionTemplate transactions;
 private final CheckoutCoordinator coordinator;
 public TransactionService(CheckoutStore checkout,CatalogClient catalog,TransactionTemplate transactions,CheckoutCoordinator coordinator) {
  this.checkout=checkout;this.catalog=catalog;this.transactions=transactions;this.coordinator=coordinator;
 }
 public TransactionResponse startTransaction(StartTransactionRequest request) {
  requireText(request.stationId(),"stationId");
  if(request.stationId().trim().length()>255) throw new BusinessException(INVALID_REQUEST,"INVALID_REQUEST","stationId too long");
  String id=UUID.randomUUID().toString(), station=request.stationId().trim();
  Instant now=Instant.now();
  checkout.create(id,station,now);
  return new TransactionResponse(id,station,"OPEN",0,new BigDecimal("0.00"),now);
 }
 public TransactionResponse getTransaction(String id) { return transactions.execute(status->checkout.find(id)); }
 public ScanResult scanItem(String id,ScanItemRequest request) {
  requireText(request.sku(),"sku");
  // 功能：本地目录快照查 SKU；scan SQL 原子检查交易存在及 OPEN 状态。
  var item=catalog.find(request.sku().trim());
  return checkout.scan(id,item);
 }
 /** 功能：将跨服务结账委托给持久化协调者，重复完成返回原收据。 */
 public Receipt completeTransaction(String id) { return coordinator.complete(id); }
 private static void requireText(String value,String name) {
  if(value==null || value.isBlank()) throw new BusinessException(INVALID_REQUEST,"INVALID_REQUEST",name+" is required");
 }
}
