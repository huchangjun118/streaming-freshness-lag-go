package monitor

import (
	"sync"
	"time"
)

// dropLogCap 订阅溢出丢弃记录的最大保留条数（环形覆盖）。
const dropLogCap = 1024

// hub 向多个订阅方广播判定结果。
// 每个订阅方有独立的有界缓冲：缓冲满时对该订阅方丢弃，
// 不阻塞其他订阅方，也不阻塞摄取路径。
type hub struct {
	mu    sync.Mutex
	subs  map[*Subscription]struct{}
	drops []DropRecord // 环形丢弃日志
	now   func() time.Time
}

func newHub(now func() time.Time) *hub {
	if now == nil {
		now = time.Now
	}
	return &hub{subs: make(map[*Subscription]struct{}), now: now}
}

// Subscription 是一个订阅句柄，C 为只读判定流。
type Subscription struct {
	C       <-chan Verdict
	c       chan Verdict
	dropped uint64
}

// Dropped 返回该订阅方因消费过慢被丢弃的判定条数。
func (s *Subscription) Dropped() uint64 { return s.dropped }

// publish 非阻塞地广播一条判定。
func (h *hub) publish(v Verdict) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs {
		select {
		case s.c <- v:
		default:
			s.dropped++
			h.appendDrop(DropRecord{
				Partition: v.Partition,
				Reason:    DropReasonSubscriberOverflow,
				Detail:    "subscriber buffer full, verdict dropped",
				At:        h.now(),
			})
		}
	}
}

func (h *hub) appendDrop(d DropRecord) {
	if len(h.drops) < dropLogCap {
		h.drops = append(h.drops, d)
		return
	}
	// 环形覆盖最旧记录。
	copy(h.drops, h.drops[1:])
	h.drops[len(h.drops)-1] = d
}

// subscribe 注册一个订阅方，buf 为其独立缓冲长度。
func (h *hub) subscribe(buf int) *Subscription {
	if buf <= 0 {
		buf = 1
	}
	c := make(chan Verdict, buf)
	s := &Subscription{C: c, c: c}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

// unsubscribe 注销并关闭订阅通道。
func (h *hub) unsubscribe(s *Subscription) {
	h.mu.Lock()
	if _, ok := h.subs[s]; ok {
		delete(h.subs, s)
		close(s.c)
	}
	h.mu.Unlock()
}

// dropRecords 返回订阅溢出丢弃记录快照。
func (h *hub) dropRecords() []DropRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]DropRecord, len(h.drops))
	copy(out, h.drops)
	return out
}
