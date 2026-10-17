package edu.cs6510.servicebased.checkout;

import edu.cs6510.servicebased.shared.contract.*;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/** 功能：保持教授 load client 使用的交易 URL 和响应状态码不变。 */
@RestController
@RequestMapping("/transactions")
public class TransactionController {
    private final TransactionService checkout;

    // 功能：注入本进程结账业务对象。
    public TransactionController(TransactionService checkout) { this.checkout = checkout; }

    // 功能：POST 创建交易；成功按 API 合同返回 201。
    @PostMapping
    public ResponseEntity<TransactionResponse> start(@Valid @RequestBody StartTransactionRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(checkout.startTransaction(request));
    }

    // 功能：GET 查询指定交易状态。
    @GetMapping("/{transactionId}")
    public TransactionResponse get(@PathVariable String transactionId) { return checkout.getTransaction(transactionId); }

    // 功能：POST 扫描一件商品；输入校验通过后执行原子扫描。
    @PostMapping("/{transactionId}/items")
    public ScanResult scan(@PathVariable String transactionId, @Valid @RequestBody ScanItemRequest request) {
        return checkout.scanItem(transactionId, request);
    }

    // 功能：POST 完成购物并返回收据；重复完成返回业务冲突。
    @PostMapping("/{transactionId}/complete")
    public Receipt complete(@PathVariable String transactionId) { return checkout.completeTransaction(transactionId); }
}
