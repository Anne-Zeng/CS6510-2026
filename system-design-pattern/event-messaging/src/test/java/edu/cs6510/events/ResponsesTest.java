package edu.cs6510.events;

import jakarta.validation.ConstraintViolationException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import java.util.Map;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class ResponsesTest {
 public enum Kind { INVALID_REQUEST,NOT_FOUND,CONFLICT }
 /** Models a service-owned exception without depending on any service module. */
 public static final class DomainFailure extends RuntimeException {
  private final Kind kind;
  public DomainFailure(Kind kind) { super("domain failure");this.kind=kind; }
  public Kind getKind() { return kind; }
  public String getCode() { return "DOMAIN_CODE"; }
 }
 public record Payload(String transactionId) {}
 private Map<?,?> error(Reply reply) {
  var body=Json.read(reply.body(),Map.class);
  assertEquals(Set.of("error","message"),body.keySet(),"OpenAPI ApiError required fields");
  return body;
 }
 @Test void preservesCreatedStatusAndStructuredSuccessBody() {
  var reply=Responses.call(201,()->new Payload("tx-1"));
  assertEquals(201,reply.status());assertEquals(new Payload("tx-1"),Json.read(reply.body(),Payload.class));
 }
 @Test void mapsEachServiceOwnedBusinessCategoryWithoutDependingOnItsClass() {
  var categories=Map.of(Kind.INVALID_REQUEST,400,Kind.NOT_FOUND,404,Kind.CONFLICT,409);
  categories.forEach((kind,status)->{
   var reply=Responses.call(()-> { throw new DomainFailure(kind); });
   assertEquals(status.intValue(),reply.status());
   assertEquals("DOMAIN_CODE",error(reply).get("error"));
   assertEquals("domain failure",error(reply).get("message"));
  });
 }
 @Test void malformedJsonReturnsValidationErrorWithoutLeakingParserDetails() {
  var reply=Responses.call(()->Json.read("{broken",Request.class));
  assertEquals(400,reply.status());assertEquals("INVALID_REQUEST",error(reply).get("error"));
  assertEquals("Invalid JSON body",error(reply).get("message"));
 }
 @Test void mapsBeanValidationFailureTo400() {
  var reply=Responses.call(()-> { throw new ConstraintViolationException("stationId is required",Set.of()); });
  assertEquals(400,reply.status());assertEquals("INVALID_REQUEST",error(reply).get("error"));
  assertEquals("stationId is required",error(reply).get("message"));
 }
 @Test void nullRequestIsRejectedBeforeCallingTheValidator() {
  var reply=Responses.call(()-> { Responses.validate(null,null);return "unreachable"; });
  assertEquals(400,reply.status());assertEquals("Request body is required",error(reply).get("message"));
 }
 @Test void keepsExplicitStatusAndReasonForKnownUnavailableOutcome() {
  var reply=Responses.call(()-> { throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"Checkout pending; retry the same transaction"); });
  assertEquals(503,reply.status());assertEquals("REQUEST_FAILED",error(reply).get("error"));
  assertEquals("Checkout pending; retry the same transaction",error(reply).get("message"));
 }
 @Test void missingStatusReasonStillProducesValidJson() {
  var reply=Responses.call(()-> { throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE); });
  assertEquals(503,reply.status());assertEquals("Request failed",error(reply).get("message"));
 }
 @Test void brokerFailureMeansUnknownBusinessOutcomeAndDoesNotExposeInfrastructure() {
  var reply=Responses.call(()-> { throw new BusException("private-kafka-host.internal:9092"); });
  assertEquals(503,reply.status());assertEquals("SERVICE_UNAVAILABLE",error(reply).get("error"));
  assertFalse(reply.body().contains("private-kafka-host"));
 }
 @Test void unexpectedFailuresHideImplementationDetails() {
  var reply=Responses.call(()-> { throw new IllegalStateException("jdbc password=secret"); });
  assertEquals(500,reply.status());assertEquals("INTERNAL_ERROR",error(reply).get("error"));
  assertFalse(reply.body().contains("secret"));
 }
}
