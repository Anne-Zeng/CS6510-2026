package edu.cs6510.servicebased.checkout;

import edu.cs6510.servicebased.checkout.persistence.SagaStore;
import edu.cs6510.servicebased.checkout.persistence.SagaStore.Work;
import edu.cs6510.servicebased.checkout.contract.Receipt;
import edu.cs6510.servicebased.checkout.common.BusinessException;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.server.ResponseStatusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;
import static edu.cs6510.servicebased.checkout.common.BusinessException.Kind.CONFLICT;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
// 协调结账过程，处理库存成功、拒绝、暂时失败和恢复
/** 功能：正常结账使用一次冻结上下文；仅持久化必要决定，恢复时重建同样上下文。 */
@Service
public class CheckoutCoordinator {
 private static final Logger log=LoggerFactory.getLogger(CheckoutCoordinator.class);
 private final SagaStore store;
 private final InventoryClient inventory;
 public CheckoutCoordinator(SagaStore store,InventoryClient inventory) { this.store=store;this.inventory=inventory; }
 public Receipt complete(String transactionId) {
  long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(7);
  do {
   var started=store.begin(transactionId);//冻结购物篮，记录结账尝试，调用SagaStore.begin()，对新的有效购物篮，它执行一条原子 SQL
   if(started.receipt()!=null) return started.receipt();
   if(started.work()!=null) {
    Receipt receipt;
    try { receipt=advance(started.work()); }//处理库存预留、确认或释放，调用CheckoutCoordinator.advance()，它根据 Saga 状态调用 InventoryClient.reserve()、confirm() 或 release()
    catch(BusinessException ex) {
     store.failed(started.work(),ex);
     throw ex;
    }
    catch(ResponseStatusException ex) {
     store.failed(started.work(),ex);
     throw ex;
    }
    catch(Exception ex) {
     store.failed(started.work(),ex);
     throw new ResponseStatusException(SERVICE_UNAVAILABLE,"Checkout pending; retry the same transaction",ex);
    }

    if(receipt==null) throw new BusinessException(CONFLICT,"INVENTORY_REJECTED","Inventory reservation rejected; basket is OPEN");
    return receipt;
   }
   // 功能：仅并发重复请求等待已有租约，或并发扫描刚改变购物篮时重新冻结；正常新交易不轮询状态。
   try { Thread.sleep(20); }
   catch(InterruptedException ex) { Thread.currentThread().interrupt();break; }
  } while(System.nanoTime()<deadline);
  throw new ResponseStatusException(SERVICE_UNAVAILABLE,"Checkout pending; retry the same transaction");
 }
 public void drive(String id) {
  var work=store.claim(id);
  if(work==null) return;
  try { advance(work); }
  catch(Exception ex) { store.failed(work,ex);throw ex; }
 }
 private Receipt advance(Work work) {
  if(work.state().equals("RESERVING")) {
   String reserved=null;
   //InventoryClient.reserve() 会等待库存业务响应，返回成功状态后，advance() 才执行下一行 store.finish(work)，并返回收据
   // 如果库存拒绝，advance() 会调用 store.decide(work,"CANCELLING")，然后继续执行下一行 store.finish(work)，并返回 null
   try { reserved=inventory.reserve(work.id(),work.lines()); }//work.id：Saga ID，reserved这个方法尚未返回时，下面的 if 和 finish() 都不会执行
   catch(InventoryClient.Rejected ex) { work=store.decide(work,"CANCELLING"); }
   if(reserved!=null) {
    // 功能：库存扣减即确认，是本 Saga 唯一的远程关键步骤；之后只剩可重试的本地完成，无需再持久化 COMMITTING。
    if(reserved.equals("RESERVED")) inventory.confirm(work.id());
    return store.finish(work);//store 是 SagaStore。它执行 Checkout 自己的 SQL：
   }
  }
  switch(work.state()) {
   // 功能：上一版两阶段预留已持久化的提交决定，恢复时只补发确认。
   case "COMMITTING" -> inventory.confirm(work.id());
   case "CANCELLING" -> inventory.release(work.id());
   default -> throw new IllegalStateException("Unknown saga state");
  }
  return store.finish(work);
 }
 @Scheduled(fixedDelayString="${checkout.saga.recovery-ms:1000}")
 public void recover() {
  for(String id:store.pending()) {
   try { drive(id); }
   catch(Exception ex) { log.warn("Checkout attempt {} remains pending: {}",id,ex.toString()); }
  }
 }
}
