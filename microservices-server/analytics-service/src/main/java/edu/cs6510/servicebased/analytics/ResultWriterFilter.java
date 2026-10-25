package edu.cs6510.servicebased.analytics;

import edu.cs6510.servicebased.analytics.PipelineData.RankedWindow;
import org.springframework.stereotype.Component;

import static edu.cs6510.servicebased.analytics.PipelineData.*;

/** 第三段：将计算完成的排名交给独立事务组件保存。 */
@Component
public class ResultWriterFilter {
    // 单独的 Spring Bean 保证调用 save 时经过 @Transactional 代理。
    private final AnalyticsWindowStore store;

    /** 注入负责原子写入窗口与排名的存储组件。 */
    public ResultWriterFilter(AnalyticsWindowStore store) {
        this.store = store;
    }

    /** 返回时数据库事务已提交，后台线程才能安全消费下一个窗口。 */
    public void write(RankedWindow window) {
        store.save(window, WINDOW_SIZE, SLIDE_INTERVAL, TOP_LIMIT);
    }
}
