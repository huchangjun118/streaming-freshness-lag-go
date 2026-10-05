package monitor

import (
	"fmt"
	"sync"
	"time"
)

// Snapshot 是发布给订阅方的一次监控结果。
type Snapshot struct {
	Metrics []PartitionMetrics
	Events  []Event
}

// Subscription 代表一个订阅方，各自独立按自己的进度读取。
type Subscription struct {
	id   int
	ch   chan Snapshot
	hub  *hub
	once sync.Once
}

// C 返回快照通道。
func (s *Subscription) C() <-chan Snapshot { return s.ch }

// Close 取消订阅并释放资源，幂等。
func (s *Subscription) Close() {
	s.once.Do(func() {
		s.hub.mu.Lock()
		delete(s.hub.subs, s.id)
		s.hub.mu.Unlock()
	})
}

// hub 将监控结果扇出给多个订阅方。慢订阅方走丢帧策略（丢弃最旧帧），
// 不阻塞快订阅方，也不拖慢摄取。
type hub struct {
	mu   sync.Mutex
	subs map[int]*Subscription
	next int
	now  func() time.Time
}

func newHub() *hub {
	return &hub{subs: make(map[int]*Subscription), now: time.Now}
}

// subscribe 注册一个缓冲容量为 size 的订阅。
func (h *hub) subscribe(size int) *Subscription {
	if size <= 0 {
		size = 1
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.next++
	s := &Subscription{id: h.next, ch: make(chan Snapshot, size), hub: h}
	h.subs[s.id] = s
	return s
}

// publish 非阻塞地向所有订阅方广播；订阅方缓冲满时丢弃其最旧一帧
// 并放入最新帧，同时产生一条可区分的丢弃记录。
func (h *hub) publish(s Snapshot) []DropRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	var drops []DropRecord
	for _, sub := range h.subs {
		select {
		case sub.ch <- s:
		default:
			// 慢订阅方：丢弃最旧帧腾位，保证不阻塞。
			select {
			case <-sub.ch:
			default:
			}
			select {
			case sub.ch <- s:
			default:
			}
			drops = append(drops, DropRecord{
				Reason: DropSubscriberSlow,
				Detail: fmt.Sprintf("subscriber %d buffer full, oldest snapshot dropped", sub.id),
				At:     h.now(),
			})
		}
	}
	return drops
}

// close 关闭所有订阅通道。
func (h *hub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, sub := range h.subs {
		close(sub.ch)
		delete(h.subs, id)
	}
}
