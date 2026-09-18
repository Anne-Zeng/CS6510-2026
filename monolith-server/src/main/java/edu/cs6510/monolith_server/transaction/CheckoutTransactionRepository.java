package edu.cs6510.monolith_server.transaction;

import org.springframework.data.jpa.repository.JpaRepository;

// The CheckoutTransactionRepository interface extends the JpaRepository interface, which provides CRUD (Create, Read, Update, Delete) operations for the CheckoutTransaction entity. 
// By extending JpaRepository, the CheckoutTransactionRepository inherits several methods for working with CheckoutTransaction persistence, including methods for saving, deleting,
// and finding CheckoutTransaction entities. The generic parameters specify that the repository will manage CheckoutTransaction entities and that the ID type of the entity is String. 
// This allows for easy interaction with the database and simplifies data access in the application.
public interface CheckoutTransactionRepository
        extends JpaRepository<CheckoutTransaction, String> {
}