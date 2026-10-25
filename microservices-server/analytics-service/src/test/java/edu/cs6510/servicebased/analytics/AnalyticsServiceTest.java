package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.analytics.common.BusinessException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/** 验证查询接口的输入边界，避免无效参数进入数据库查询。 测试环境：部分数据库组件使用 Mock；网关使用临时模拟后端*/
class AnalyticsServiceTest {
    /** 未提供 limit 时应使用旧客户端约定的前十名。 */
    @Test
    void absentLimitUsesTenAndPositiveLimitIsForwarded() {
        var store = mock(AnalyticsWindowStore.class);// 创建一个模拟的存储对象，不连接 PostgreSQL。
        var service = new AnalyticsService(store);// AnalyticsService 本身使用真实代码，但它依赖的 store 是模拟对象。
        service.getPopularItems(null);// 调用真实业务方法，分别测试默认数量和指定数量。不传 limit → AnalyticsService 应该查询前 10 名
        service.getPopularItems(3);
        verify(store).latestRanking(10, 1000, 500);// 检查业务方法是否把正确的参数传给 store。
        verify(store).latestRanking(3, 1000, 500);
    }

    /** 无效数量必须在访问存储前失败，交由统一异常处理器转换为 HTTP 400。 */
    @Test
    void invalidLimitsAreRejectedBeforeReadingStorage() {
        var store = mock(AnalyticsWindowStore.class);
        var service = new AnalyticsService(store);
        assertThrows(BusinessException.class, () -> service.getPopularItems(0));
        assertThrows(BusinessException.class, () -> service.getPopularItems(-1));
        verifyNoInteractions(store);
    }
}
