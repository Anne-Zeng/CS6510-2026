package edu.cs6510.pipeline_server.common;

/** Business failures carry no HTTP or persistence dependencies. */
public class BusinessException extends RuntimeException {
    public enum Kind { INVALID_REQUEST, NOT_FOUND, CONFLICT }
    private final Kind kind;
    private final String code;

    public BusinessException(Kind kind, String code, String message) {
        super(message);
        this.kind = kind;
        this.code = code;
    }

    public Kind getKind() { return kind; }
    public String getCode() { return code; }
}
