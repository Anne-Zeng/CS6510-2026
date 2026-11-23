package edu.cs6510.servicebased.checkout.common;

/** 功能：业务异常不绑定 HTTP；各服务统一把错误类别映射到状态码。 */

public class BusinessException extends RuntimeException {
    public enum Kind { INVALID_REQUEST, NOT_FOUND, CONFLICT }
    private final Kind kind;
    private final String code;

    // 功能：构造器：接收依赖或初始化当前对象的全部业务字段。
    public BusinessException(Kind kind, String code, String message) {
        super(message);
        this.kind = kind;
        this.code = code;
    }

    // 功能：只读访问：返回业务错误类别。
    public Kind getKind() { return kind; }
    // 功能：只读访问：返回稳定的错误代码。
    public String getCode() { return code; }
}
