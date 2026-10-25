package edu.cs6510.servicebased.checkout.common;

import edu.cs6510.servicebased.checkout.common.BusinessException;
import edu.cs6510.servicebased.checkout.contract.ApiError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/** 功能：统一处理各业务服务的校验、冲突和内部错误，不向客户端暴露堆栈。 */

@RestControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(BusinessException.class)
    // 功能：将无效请求、未找到和业务冲突转换为 400、404 和 409。
    public ResponseEntity<ApiError> business(BusinessException ex) {
        HttpStatus status = switch (ex.getKind()) {
            case INVALID_REQUEST -> HttpStatus.BAD_REQUEST;
            case NOT_FOUND -> HttpStatus.NOT_FOUND;
            case CONFLICT -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status).body(new ApiError(ex.getCode(), ex.getMessage()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    // 功能：提取请求字段的校验错误，以统一 JSON 返回 HTTP 400。
    public ResponseEntity<ApiError> validation(MethodArgumentNotValidException ex) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getDefaultMessage()).findFirst().orElse("Invalid request");
        return ResponseEntity.badRequest().body(new ApiError("INVALID_REQUEST", message));
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class})
    // 功能：拒绝无法解析的 JSON 或类型错误的查询参数。
    public ResponseEntity<ApiError> malformed(Exception ex) {
        return ResponseEntity.badRequest().body(
                new ApiError("INVALID_REQUEST", "Invalid JSON body or query parameter"));
    }

    @ExceptionHandler(NoResourceFoundException.class)
    // 功能：将不存在的 HTTP 路径转换为统一的 404 JSON。
    public ResponseEntity<ApiError> missing(NoResourceFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ApiError("NOT_FOUND", "Endpoint not found"));
    }

    // 功能：保留上游不可用与未实现接口的明确 HTTP 状态。
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiError> status(ResponseStatusException ex) {
        return ResponseEntity.status(ex.getStatusCode()).body(new ApiError("REQUEST_FAILED", ex.getReason()));
    }

    @ExceptionHandler(Exception.class)
    // 功能：记录内部异常供排查，对客户端只返回通用 500 错误。
    public ResponseEntity<ApiError> unexpected(Exception ex) {
        log.error("Unexpected request failure", ex);
        return ResponseEntity.internalServerError()
                .body(new ApiError("INTERNAL_ERROR", "Unexpected server error"));
    }
}
