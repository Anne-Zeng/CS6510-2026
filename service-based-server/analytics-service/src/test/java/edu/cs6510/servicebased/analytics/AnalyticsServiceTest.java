package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.shared.common.BusinessException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/** 验证查询接口的输入边界，避免无效参数进入数据库查询。 */
class AnalyticsServiceTest {
    /** 未提供 limit 时应使用旧客户端约定的前十名。 */
    @Test
    void absentLimitUsesTenAndPositiveLimitIsForwarded() {
        var store = mock(AnalyticsWindowStore.class);
        var service = new AnalyticsService(store);
        service.getPopularItems(null);
        service.getPopularItems(3);
        verify(store).latestRanking(10, 1000, 500);
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
