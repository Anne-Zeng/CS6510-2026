package edu.cs6510.monolith_server.transaction;

import edu.cs6510.monolith_server.transaction.api.ScanItemRequest;
import edu.cs6510.monolith_server.transaction.api.Receipt;
import edu.cs6510.monolith_server.transaction.api.ScanResult;
import edu.cs6510.monolith_server.transaction.api.StartTransactionRequest;
import edu.cs6510.monolith_server.transaction.api.TransactionResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

// The TransactionController class is a REST controller that handles HTTP requests related to checkout transactions. It is annotated with @RestController, indicating that it is a Spring-managed controller that can handle RESTful web service requests. The class is mapped to the "/transactions" URL path using the @RequestMapping annotation, which means that all endpoints defined in this controller will be prefixed with "/transactions".
// The class has a dependency on the CheckoutService, which is injected through the constructor and is responsible for handling the business logic related to checkout transactions. 
// The startTransaction() method is mapped to the HTTP POST method using the @PostMapping annotation, allowing clients to initiate a new checkout transaction by sending a POST request to the "/transactions" endpoint. 
// The method takes a StartTransactionRequest object as input(validated using the @Valid annotation to ensure that the required stationId field is provided). 
// The method calls the startTransaction() method of the CheckoutService to create a new transaction and returns a ResponseEntity containing a TransactionResponse object with the details of the newly created transaction, along with an HTTP status code of 201 (Created) to indicate that the transaction was successfully created.
// The TransactionController class encapsulates the RESTful API endpoints for managing checkout transactions, providing a clear and structured interface for clients to interact with the transaction system while delegating the business logic to the CheckoutService.
@RestController
@RequestMapping("/transactions")
public class TransactionController {

    private final CheckoutService checkoutService;

    public TransactionController(CheckoutService checkoutService) {
        this.checkoutService = checkoutService;
    }

    @GetMapping("/{transactionId}")
    public ResponseEntity<TransactionResponse> getTransaction(
        @PathVariable String transactionId
    ) {
        return ResponseEntity.ok(
            checkoutService.getTransaction(transactionId)
        );
    }

    @PostMapping
    public ResponseEntity<TransactionResponse> startTransaction(
            @Valid @RequestBody StartTransactionRequest request
    ) {
        //TransactionResponse response = checkoutService.startTransaction(request);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(checkoutService.startTransaction(request));
    }

     @PostMapping("/{transactionId}/items")
    public ResponseEntity<ScanResult> scanItem(
            @PathVariable String transactionId,
            @Valid @RequestBody ScanItemRequest request
    ) {
        //ScanResult response = checkoutService.scanItem(transactionId, request);
        return ResponseEntity.ok(checkoutService.scanItem(transactionId, request));
    }

     @PostMapping("/{transactionId}/complete")
    public ResponseEntity<Receipt> completeTransaction(
            @PathVariable String transactionId
    ) {
        return ResponseEntity.ok(
                checkoutService.completeTransaction(transactionId)
        );
    }
}