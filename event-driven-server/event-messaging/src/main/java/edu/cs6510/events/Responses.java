package edu.cs6510.events;

import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import org.springframework.web.server.ResponseStatusException;
import java.util.Map;
import java.util.function.Supplier;

/** Maps local domain outcomes to the unchanged external API contract, without making HTTP calls. */
public final class Responses {
    private Responses() {}
    public static Reply call(Supplier<?> action) { return call(200, action); }
    public static Reply call(int status, Supplier<?> action) {
        try { return new Reply(status, Json.write(action.get())); }
        catch (ResponseStatusException ex) { return error(ex.getStatusCode().value(), "REQUEST_FAILED", ex.getReason()); }
        catch (ConstraintViolationException | IllegalArgumentException ex) { return error(400, "INVALID_REQUEST", ex.getMessage()); }
        catch (tools.jackson.core.JacksonException ex) { return error(400, "INVALID_REQUEST", "Invalid JSON body"); }
        catch (RuntimeException ex) {
            // Domain services own their exception classes; avoid sharing persistence or depending on a service module.
            try {
                String kind = ex.getClass().getMethod("getKind").invoke(ex).toString();
                String code = ex.getClass().getMethod("getCode").invoke(ex).toString();
                int statusCode = switch (kind) { case "INVALID_REQUEST" -> 400; case "NOT_FOUND" -> 404; case "CONFLICT" -> 409; default -> 500; };
                return error(statusCode, code, ex.getMessage());
            } catch (ReflectiveOperationException ignored) {
                if (ex instanceof BusException) return error(503, "SERVICE_UNAVAILABLE", "Business outcome unavailable; retry the same transaction");
                System.err.println("Message handler failed: " + ex.getClass().getSimpleName());
                return error(500, "INTERNAL_ERROR", "Unexpected server error");
            }
        }
    }
    public static Reply error(int status, String code, String message) {
        return new Reply(status, Json.write(Map.of("error", code, "message", message == null ? "Request failed" : message)));
    }
    public static void validate(Validator validator, Object request) {
        if (request == null) throw new IllegalArgumentException("Request body is required");
        var violations = validator.validate(request);
        if (!violations.isEmpty()) throw new ConstraintViolationException(violations);
    }
}
