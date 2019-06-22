package pool

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"workshop/coarsetime"
)

var (
	defaultWorkerMaxQuota        = 64              // 默认的最大资源数 64
	defaultWorkerMaxIdleDuration = 3 * time.Minute // 默认的最大空闲时长 3分钟
)
var (
	// ErrWorkshopClosed error : 'workshop is closed'
	ErrWorkshopClosed = fmt.Errorf("%s", "workship is closed")
)

/**
 * Non-blocking asynchronous multiplex resource pool.
 *
 * Conditions of Use:
 * - Limited resources
 * - Resources can be multiplexed non-blockingly and asynchronously
 * - Typical application scenarios, such as connection pool for asynchronous communication
 * - 资源有限的
 * - 资源可以被多路复用 - 非阻塞 异步多路复用
 * - 最佳应用场景应该是在做连接池的时候
 *
 * Performance:
 * - The longer the business is, the more obvious the performance improvement.
 * If the service is executed for 1ms each time, the performance is improved by about 4 times;
 * If the business is executed for 10ms each time, the performance is improved by about 28 times
 * - The average time spent on each operation will not change significantly,
 * but the overall throughput is greatly improved
 *
 * 性能
 * 业务耗时越长,性能优势就会越明显, 比如 mysql 的慢查询, 资源可以有效利用
 */
type (
	// Worker worker interface
	// Note: Worker can not be implemented using empty structures(struct{})!
	Worker interface { // 资源 - 可被同时使用
		Health() bool // 健康状态
		Close() error // 解雇
	}

	// Workershop working workshop
	Workshop struct { // 车间 对 Worker 的管理以及一些统计
		addFn           func() (*workerInfo, error) // workerInfo 的创建
		maxQuota        int                         // 最大额度 - 最大可容纳多少个 Worker
		maxIdleDuration time.Duration               // 最大空闲时长 - 一个Woker 最大空闲多久 超时就 Close
		infos           map[Worker]*workerInfo      // 所有得 Worker 信息
		minLoadInfo     *workerInfo                 // 最小负载 - 记录当前最小的负载 Worker 用于每次的负载均衡
		stats           *WorkshopStats              // 状态统计 - 允许我们随时查看当前 Workshop 状态 - 内部使用
		statsReader     atomic.Value                // 外部使用的 状态统计  并不是实时更新 需要触发条件才更新
		lock            sync.Mutex                  // lock 锁
		wg              sync.WaitGroup              // wg 为了 Close 的时候可以平滑关闭
		closeCh         chan struct{}               // 关闭使用
		closeLock       sync.Mutex                  // 关闭时使用的锁
	}

	workerInfo struct { // 资源信息描述
		worker     Worker    // 资源
		jobNum     int32     // 资源当前工作的数量 - 比如当前并发处理的请求
		idleExpire time.Time // 空闲超时
	}

	// WorkshopStats workshop stats
	WorkshopStats struct { // 车间状态统计
		Worker  int32  // 当前资源总数
		Idle    int32  // 当前的空闲资源总数
		Created uint64 // 累积创建的资源总数
		Doing   int32  // 当前正在处理的资源总数
		Done    uint64 // 累积处理完成的请求数
		MaxLoad int32  // 当前状态下 最大负载的 Worker 的负载量有多大
		MinLoad int32  // 当前状态下 最小负载的 Worker 的负载量有多小
	}
)

// NewWorkshop creates a new workshop(non-blocking asynchronous multiplex resource pool).
// If maxQuota<=0, will use default value.
// If maxIdleDuration<=0, will use default value.
// Note: Worker can not be implemented using empty structures(struct{})!
func NewWorkshop(maxQuota int, maxIdleDuration time.Duration, newWorkerFunc func() (Worker, error)) *Workshop {
	// 最大资源数 最大空闲时长  new资源的方法
	if maxQuota <= 0 {
		maxQuota = defaultWorkerMaxQuota
	}
	if maxIdleDuration <= 0 {
		maxIdleDuration = defaultWorkerMaxIdleDuration
	}
	w := new(Workshop)
	w.stats = new(WorkshopStats) // 初始化状态
	w.reportStatsLocked()        // 状态上报
	w.maxQuota = maxQuota
	w.maxIdleDuration = maxIdleDuration
	w.infos = make(map[Worker]*workerInfo, maxQuota) // Worker 信息的初始化
	w.closeCh = make(chan struct{})                  // choseCh 的初始化
	w.addFn = func() (info *workerInfo, err error) { // 把传进来的初始化资源方法封装
		defer func() {
			if p := recover(); p != nil { // 异常捕获
				err = fmt.Errorf("%v", p)
			}
		}()

		worker, err := newWorkerFunc() // 生成函数 生成一个资源
		if err != nil {
			return nil, err
		}
		info = &workerInfo{ // 把生成的资源封装进 workerInfo 当中
			worker: worker,
		}
		w.infos[worker] = info // 把 workerInfo 存储在 哈希里面
		w.stats.Created++      // 状态更新 历史创建+1
		w.stats.Worker++       // 状态更新 Worker数+1
		return info, nil
	}
	go w.gc() // 定时去移除一些 超时或者不健康的 Worker
	return w
}

// Callback assigns a healthy worker to execute the function.
func (w *Workshop) Callback(fn func(Worker) error) (err error) {
	// 回调 - 传入一个函数, 函数接收一个 Worker 然后对 Worker资源的一个操作 操作完以后返回error
	select {
	case <-w.closeCh: // 检查 Worksho 是否被关闭 如果被关闭返回 ErrWorkshopClosed
		return ErrWorkshopClosed
	default:
	}
	w.lock.Lock()               // 加锁
	info, err := w.hireLocked() // 雇佣 从 Workshop 雇佣一个 Worker return workerInfo
	w.lock.Unlock()             // 解锁
	if err != nil {
		return err
	}
	worker := info.worker // 获取到 Worker
	defer func() {        // 每次使用完 Worker 之后的收尾工作
		if p := recover(); p != nil {
			err = fmt.Errorf("%v", p)
		}
		w.lock.Lock()
		_, ok := w.infos[worker] // 检查 Worker 是否还在资源池里面
		if !ok {
			worker.Close() // Worker 不在资源池里面就关闭
		} else {
			w.fireLocked(info) // 如果 Worker 仍在在就解雇 Worker
		}
		w.lock.Unlock()
	}()
	return fn(worker) // 进行回调操作
}

// Close wait for all the work to be completed and close the workshop.
func (w *Workshop) Close() { // 对外的Close 函数
	w.closeLock.Lock()
	defer w.closeLock.Unlock()
	select {
	case <-w.closeCh: // 尝试查看 closeCh 是否被关闭了
		return // 避免重复关闭
	default:
		close(w.closeCh) // 主动关闭 closeCh  其他在调用的 回调就知道这个 Workshop 已经关闭了
	}

	w.wg.Wait() // 等待所有资源被回收
	w.lock.Lock()
	defer w.lock.Unlock()
	for _, info := range w.infos {
		info.worker.Close() // 把资源池里面所有 Worker 关闭
	}
	w.infos = nil
	w.stats.Idle = 0
	w.stats.Worker = 0    // 清空 Workshop
	w.refreshLocked(true) // 刷新整个状态
}

// Fire marks the worker to reduce a job.
// If the worker does not belong to the workshop, close the worker.
func (w *Workshop) Fire(worker Worker) { // 解雇
	w.lock.Lock()
	info, ok := w.infos[worker] // 从资源池里拿到一个 Worker
	if !ok {
		if worker != nil {
			worker.Close()
		}
		w.lock.Unlock()
		return // 拿到Worker以后就返回了
	}
	w.fireLocked(info) // 解雇的过程
	w.lock.Unlock()
}

// Hire hires a healthy worker and marks the worker to add a job.
func (w *Workshop) Hire() (Worker, error) { // 雇佣  取到一个 Worker
	select {
	case <-w.closeCh:
		return nil, ErrWorkshopClosed
	default:
	}

	w.lock.Lock()
	info, err := w.hireLocked() // 取 Worker 的过程
	if err != nil {
		w.lock.Unlock()
		return nil, err
	}
	w.lock.Unlock()
	return info.worker, nil
}

// Stats returns the current workshop stats.
func (w *Workshop) Stats() WorkshopStats { // 从 statsReader 中 原子读一个 WorkshopStats
	return w.statsReader.Load().(WorkshopStats)
}

func (w *Workshop) fireLocked(info *workerInfo) { // 解雇过程
	{
		w.stats.Doing-- //更新 Workshop 的状态 正在工作的-1
		w.stats.Done++  // Workshop 的状态 已完成工作 +1
		info.jobNum--   // Woker 正在工作 -1
		w.wg.Add(-1)    // wg -1
	}
	jobNum := info.jobNum
	if jobNum == 0 { // 对当前工作数判断
		info.idleExpire = coarsetime.CeilingTimeNow().Add(w.maxIdleDuration) // 设置空闲超时
		w.stats.Idle++                                                       // 对 Workshop 的空闲资源 +1
	}

	if jobNum+1 >= w.stats.MaxLoad { // +1 还原工作状态判断是否最大负载
		w.refreshLocked(true) // 刷新状态
		return
	}

	if !w.checkInfoLocked(info) { // 判断 Worker 健康状态
		if info == w.minLoadInfo { // 如果Worker 是最小负载 就上报刷新状态
			w.refreshLocked(true)
		}
		return
	}

	if jobNum < w.stats.MinLoad { // 判断当前 任务数 是否最小
		w.stats.MinLoad = jobNum // 更新当前最小任务数
		w.minLoadInfo = info     // 更新当前最小负载 Worker
	}
	w.reportStatsLocked() // 状态上报
}

func (w *Workshop) hireLocked() (*workerInfo, error) { // 雇佣过程
	var info *workerInfo
GET:
	info = w.minLoadInfo // 首先拿到最小 Worker 负载的信息
	if len(w.infos) >= w.maxQuota || (info != nil && info.jobNum == 0) {
		// 判断当前资源池是否已经到达最大额度
		// 判断 Workerinfo 是否空  当前是否没有任何任务
		if !w.checkInfoLocked(info) { // 检查 workerInfo 的健康状态
			w.refreshLocked(false) // 如果不健康就进行刷新  params: 报告状态 true/false
			goto GET               // 再从新 查找最小负载的信息
		}
		if info.jobNum == 0 { // 判断当前 Worker 工作数是否为0
			w.stats.Idle-- // 当前 Workshop 空闲数 -1
		}
		info.jobNum++ // 当前拿到的最小负载 Worker 工作数 +1
		w.setMinLoadInfoLocked()
		w.stats.MinLoad = w.minLoadInfo.jobNum // 把最小负载值 更新
		if w.stats.MaxLoad < info.jobNum {     // 因为当前工作 Worker 已经+1 需要判断一下是否变成最大负载了
			w.stats.MaxLoad = info.jobNum // 更新最大负载
		}
	} else { // 如果资源池没有满  会做一个 Worker 的增加
		var err error
		info, err = w.addFn() // 生成一个 Worker
		if err != nil {
			return nil, err
		}
		info.jobNum = 1 // 给 Worker 的工作 +1
		w.stats.MinLoad = 1
		if w.stats.MaxLoad == 0 {
			w.stats.MaxLoad = 1
		}
		w.minLoadInfo = info
	}

	w.wg.Add(1)           // 作为一个资源计数 +1
	w.stats.Doing++       // Workshop 正在处理的任务数+1
	w.reportStatsLocked() // 上报状态

	return info, nil
}

func (w *Workshop) gc() { // 定时去移除一些 超时或者不健康的 Worker
	for {
		select {
		case <-w.closeCh: // 先检查 Workshop 是否被关闭
			return
		default:
			time.Sleep(w.maxIdleDuration) // Sleep 最大空闲时长
			w.lock.Lock()                 // 加锁
			w.refreshLocked(true)         // 刷新当前所有资源检测状态
			w.lock.Unlock()               // 解锁
		}
	}
}

func (w *Workshop) setMinLoadInfoLocked() { // 设置最小负载 Worker  -  在雇佣的时候 需要刷新
	if len(w.infos) == 0 { // 如果当前资源池为 空 就设置最小负载为 空
		w.minLoadInfo = nil
		return
	}
	var minLoadInfo *workerInfo
	for _, info := range w.infos { // 遍历 找出最小 Worker
		if minLoadInfo != nil && info.jobNum >= minLoadInfo.jobNum {
			continue
		}
		minLoadInfo = info
	}
	w.minLoadInfo = minLoadInfo // 更新 Workshop 最小负载 Worker
}

// Remove the expired or unhealthy idel workers.
// The time complexity is O(n).
func (w *Workshop) refreshLocked(reportStats bool) {
	var max, min, tmp int32
	min = math.MaxInt32
	var minLoadInfo *workerInfo    // 找到最小负载的Worker
	for _, info := range w.infos { // 循环 遍历资源池
		if !w.checkInfoLocked(info) { // 每一个 Worker 都检查一下健康状态
			continue // 如果不健康 就默认移除了
		}
		tmp = info.jobNum // 记录当前工作的数量
		if tmp > max {    // 找出整个 Workshop 的最大值
			max = tmp
		}
		if tmp < min { // 找出整个 Workshop 的最小值
			min = tmp
		}
		if minLoadInfo != nil && tmp >= minLoadInfo.jobNum { // 判断当前的循环的 Worker 是否最小负载的 Worker
			continue
		}
		minLoadInfo = info // 找到最小负载的Worker
	}
	if min == math.MaxInt32 { // 判断 min 是否等于一开始的赋值
		min = 0 // 代表现在没有 Worker
	}
	w.stats.MinLoad = min // 更新 Workershop 最小负载
	w.stats.MaxLoad = max // 更新 Workershop 最大负载
	if reportStats {      // 上报的的状态  true 就更新上报当前状态  false 不更新上报当前状态
		w.reportStatsLocked()
	}
	w.minLoadInfo = minLoadInfo // 把当前的最小负载更新到 Workershop
}

func (w *Workshop) checkInfoLocked(info *workerInfo) bool { // 检查 Worker 是否健康的流程
	if !info.worker.Health() || (info.jobNum == 0 && coarsetime.FloorTimeNow().After(info.idleExpire)) {
		// 检查 Worker 的健康函数 或者 (Worker 工作数为 0 并且 已经空闲超时 )
		delete(w.infos, info.worker) // 移除 并关闭 Worker
		info.worker.Close()
		w.stats.Worker--
		if info.jobNum == 0 { // 如果 Worker 的工作数是 0
			w.stats.Idle-- // Workshop 空闲数要  -1
		} else { // 该逻辑属于维护 Workshop 的状态
			w.wg.Add(-int(info.jobNum))         // 如果正在有工作在做  wg要 减掉该 Worker 的工作数
			w.stats.Doing -= info.jobNum        // Doing 要减掉他正在做得工作
			w.stats.Done += uint64(info.jobNum) // Done 要加上他正在做得
		}
		return false // 不健康 return false
	}
	return true // 健康 return true
}

func (w *Workshop) reportStatsLocked() {
	w.statsReader.Store(*w.stats) // 原子操作  因为不用加锁 对性能不会有影响
}
