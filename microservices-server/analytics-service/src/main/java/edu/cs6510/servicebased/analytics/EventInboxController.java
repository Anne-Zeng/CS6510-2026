package edu.cs6510.servicebased.analytics;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;
/** 功能：内部事件投递端点；返回 200 时整个批次已在 Analytics 数据库提交。 */
@RestController
public class EventInboxController {
 private final EventInbox inbox;
 public EventInboxController(EventInbox inbox) { this.inbox=inbox; }
 @PostMapping("/internal/events/scans")
 public EventInbox.Ack receive(@Valid @RequestBody EventInbox.Batch batch) { return inbox.accept(batch); }
}
