package edu.cs6510.servicebased.shared.persistence.entity;

/** 功能：交易状态枚举；数据库按字符串保存，便于查看和审计。 */

public enum TransactionStatus {
    OPEN,
    COMPLETED,
    CANCELLED
}
