package edu.cs6510.servicebased.checkout.persistence;

import edu.cs6510.events.Reply;
import edu.cs6510.servicebased.checkout.common.BusinessException;
import edu.cs6510.servicebased.checkout.contract.CatalogItemResponse;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CommandInboxTest {
    JdbcTemplate jdbc=mock(JdbcTemplate.class);
    CommandInbox inbox=new CommandInbox(jdbc);
    CatalogItemResponse item=new CatalogItemResponse("A","Apple",new BigDecimal("1.25"));

    @Test void normalScanWritesBasketOutboxAndReplayResponseInOneDatabaseCall() {
        var committed=new Reply(200,"{\"itemCount\":1}");
        queryReturns(List.of(committed));
        assertEquals(committed,inbox.scan("request-1","basket-1",item));
        verify(jdbc,times(1)).query(anyString(),any(RowMapper.class),any(Object[].class));
        verifyNoMoreInteractions(jdbc);
    }

    @Test void normalStartCreatesBasketAndReplayResponseInOneDatabaseCall() {
        var committed=new Reply(201,"{\"transactionId\":\"basket-1\"}");
        queryReturns(List.of(committed));
        assertEquals(committed,inbox.start("request-1","station-1"));
        verify(jdbc,times(1)).query(anyString(),any(RowMapper.class),any(Object[].class));
        verifyNoMoreInteractions(jdbc);
    }

    @Test void concurrentUniqueRequestConflictReadsWinnerAfterAtomicStatementRollback() {
        var committed=new Reply(200,"{\"itemCount\":1}");
        queryThrowsThenReturns(new DuplicateKeyException("command_results_pkey"),List.of(committed));
        assertEquals(committed,inbox.scan("request-1","basket-1",item));
        verify(jdbc).query(eq("SELECT status,body FROM command_results WHERE request_id=?"),
                any(RowMapper.class),eq("request-1"));
    }

    @Test void unrelatedConstraintFailureIsNotMistakenForACommittedReplay() {
        var failure=new DuplicateKeyException("some_other_constraint");
        queryThrowsThenReturns(failure,List.of());
        assertSame(failure,assertThrows(DuplicateKeyException.class,
                ()->inbox.start("request-1","station-1")));
    }

    @Test void failedDatabaseStatementNeverReturnsASuccessfulReply() {
        var failure=new DataAccessResourceFailureException("database disconnected");
        doThrow(failure).when(jdbc).query(anyString(),any(RowMapper.class),any(Object[].class));
        assertSame(failure,assertThrows(DataAccessResourceFailureException.class,
                ()->inbox.scan("request-1","basket-1",item)));
        verify(jdbc,times(1)).query(anyString(),any(RowMapper.class),any(Object[].class));
    }

    @Test void replayCanBeRecoveredWhenBasketCompletedAfterTheStatementSnapshot() {
        var committed=new Reply(200,"{\"itemCount\":1}");
        queryReturns(Arrays.asList((Reply)null),List.of(committed));
        assertEquals(committed,inbox.scan("request-1","basket-1",item));
    }

    @Test void closedBasketRemainsAConflictWhenThereIsNoCommittedCommand() {
        queryReturns(Arrays.asList((Reply)null),List.of());
        var failure=assertThrows(BusinessException.class,()->inbox.scan("request-1","basket-1",item));
        assertEquals("TRANSACTION_NOT_OPEN",failure.getCode());
    }

    @Test void absentBasketRemainsNotFoundWhenThereIsNoCommittedCommand() {
        queryReturns(List.of(),List.of());
        var failure=assertThrows(BusinessException.class,()->inbox.scan("request-1","basket-1",item));
        assertEquals("TRANSACTION_NOT_FOUND",failure.getCode());
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    private void queryReturns(List<Reply> first,List<Reply>... next) {
        var result=doReturn(first);
        for(var value:next) result=result.doReturn(value);
        result.when(jdbc).query(anyString(),any(RowMapper.class),any(Object[].class));
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    private void queryThrowsThenReturns(RuntimeException failure,List<Reply> next) {
        doThrow(failure).doReturn(next).when(jdbc).query(anyString(),any(RowMapper.class),any(Object[].class));
    }
}
