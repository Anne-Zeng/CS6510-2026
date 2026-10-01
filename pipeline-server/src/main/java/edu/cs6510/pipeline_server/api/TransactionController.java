package edu.cs6510.pipeline_server.api;

import edu.cs6510.pipeline_server.contract.*;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/transactions")
public class TransactionController {
    private final CheckoutFacade checkout;
    public TransactionController(CheckoutFacade checkout) { this.checkout = checkout; }

    @PostMapping
    public ResponseEntity<TransactionResponse> start(
            @Valid @RequestBody StartTransactionRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(checkout.startTransaction(request));
    }

    @GetMapping("/{transactionId}")
    public TransactionResponse get(@PathVariable String transactionId) {
        return checkout.getTransaction(transactionId);
    }

    @PostMapping("/{transactionId}/items")
    public ScanResult scan(@PathVariable String transactionId,
                           @Valid @RequestBody ScanItemRequest request) {
        return checkout.scanItem(transactionId, request);
    }

    @PostMapping("/{transactionId}/complete")
    public Receipt complete(@PathVariable String transactionId) {
        return checkout.completeTransaction(transactionId);
    }
}
