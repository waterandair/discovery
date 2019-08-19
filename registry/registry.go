package registry

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/bilibili/discovery/conf"
	"github.com/bilibili/discovery/model"

	"github.com/bilibili/kratos/pkg/ecode"
	"github.com/bilibili/kratos/pkg/log"
)

const (
	_evictThreshold = int64(90 * time.Second)
	_evictCeiling   = int64(3600 * time.Second)
)

// Registry 处理同步所有操作到对等的节点
// Registry handles replication of all operations to peer Discovery nodes to keep them all in sync.
type Registry struct {
	appm  map[string]*model.Apps // appid-env -> apps
	aLock sync.RWMutex

	conns     map[string]map[string]*conn // region.zone.env.appid-> host
	cLock     sync.RWMutex
	scheduler *scheduler
	gd        *Guard
}

// conn the poll chan contains consumer.
type conn struct {
	ch         chan map[string]*model.InstanceInfo // TODO(felix): increase
	arg        *model.ArgPolls
	latestTime int64
	count      int
}

// newConn new consumer chan.
func newConn(ch chan map[string]*model.InstanceInfo, latestTime int64, arg *model.ArgPolls) *conn {
	return &conn{ch: ch, latestTime: latestTime, arg: arg, count: 1}
}

// NewRegistry new register.
func NewRegistry(conf *conf.Config) (r *Registry) {
	r = &Registry{
		appm:  make(map[string]*model.Apps),
		conns: make(map[string]map[string]*conn),
		gd:    new(Guard),
	}
	r.scheduler = newScheduler(r)
	r.scheduler.Load(conf.Scheduler)

	// 自我保护
	go r.proc()
	return
}

func (r *Registry) newApps(appid, env string) (a *model.Apps, ok bool) {
	key := appsKey(appid, env)
	r.aLock.Lock()
	if a, ok = r.appm[key]; !ok {
		a = model.NewApps()
		r.appm[key] = a
	}
	r.aLock.Unlock()
	return
}

func (r *Registry) apps(appid, env, zone string) (as []*model.App, a *model.Apps, ok bool) {
	key := appsKey(appid, env)
	r.aLock.RLock()
	a, ok = r.appm[key]
	r.aLock.RUnlock()
	if ok {
		as = a.App(zone)
	}
	return
}

func appsKey(appid, env string) string {
	return fmt.Sprintf("%s-%s", appid, env)
}

func (r *Registry) newApp(ins *model.Instance) (a *model.App) {
	as, _ := r.newApps(ins.AppID, ins.Env)
	a, _ = as.NewApp(ins.Zone, ins.AppID, ins.LatestTimestamp)
	return
}

// 注册一个实例
// Register a new instance.
func (r *Registry) Register(ins *model.Instance, latestTime int64) (err error) {
	a := r.newApp(ins)
	i, ok := a.NewInstance(ins, latestTime)
	if ok {
		// 增加心跳期望数
		r.gd.incrExp()
	}
	// 确保在更新 app 的 latest timestamp 前清空 poll 请求
	// NOTE: make sure free poll before update appid latest timestamp.
	r.broadcast(i.Env, i.AppID)
	return
}

// Renew marks the given instance of the given app name as renewed, and also marks whether it originated from replication.
func (r *Registry) Renew(arg *model.ArgRenew) (i *model.Instance, ok bool) {
	a, _, _ := r.apps(arg.AppID, arg.Env, arg.Zone)
	if len(a) == 0 {
		return
	}
	if i, ok = a[0].Renew(arg.Hostname); !ok {
		return
	}
	r.gd.incrFac()
	return
}

// Cancel cancels the registration of an instance.
func (r *Registry) Cancel(arg *model.ArgCancel) (i *model.Instance, ok bool) {
	if i, ok = r.cancel(arg.Zone, arg.Env, arg.AppID, arg.Hostname, arg.LatestTimestamp); !ok {
		return
	}
	r.gd.decrExp()
	return
}

func (r *Registry) cancel(zone, env, appid, hostname string, latestTime int64) (i *model.Instance, ok bool) {
	var l int
	a, as, _ := r.apps(appid, env, zone)
	if len(a) == 0 {
		return
	}
	if i, l, ok = a[0].Cancel(hostname, latestTime); !ok {
		return
	}
	as.UpdateLatest(latestTime)
	if l == 0 {
		if a[0].Len() == 0 {
			as.Del(zone)
		}
	}
	if len(as.App("")) == 0 {
		r.aLock.Lock()
		delete(r.appm, appsKey(appid, env))
		r.aLock.Unlock()
	}
	r.broadcast(env, appid) // NOTE: make sure free poll before update appid latest timestamp.
	return
}

// FetchAll fetch all instances of all the families.
func (r *Registry) FetchAll() (im map[string][]*model.Instance) {
	ass := r.allApp()
	im = make(map[string][]*model.Instance)
	for _, as := range ass {
		for _, a := range as.App("") {
			im[a.AppID] = append(im[a.AppID], a.Instances()...)
		}
	}
	return
}

// Fetch fetch all instances by appid.
func (r *Registry) Fetch(zone, env, appid string, latestTime int64, status uint32) (info *model.InstanceInfo, err error) {
	key := appsKey(appid, env)
	r.aLock.RLock()
	a, ok := r.appm[key]
	r.aLock.RUnlock()
	if !ok {
		err = ecode.NothingFound
		return
	}
	info, err = a.InstanceInfo(zone, latestTime, status)
	if err != nil {
		return
	}
	sch := r.scheduler.Get(appid, env)
	if sch != nil {
		info.Scheduler = sch.Zones
	}
	return
}

// Polls hangs request and then write instances when that has changes, or return NotModified.
func (r *Registry) Polls(arg *model.ArgPolls) (ch chan map[string]*model.InstanceInfo, new bool, miss string, err error) {
	var (
		ins = make(map[string]*model.InstanceInfo, len(arg.AppID))
		in  *model.InstanceInfo
	)
	if len(arg.AppID) != len(arg.LatestTimestamp) {
		arg.LatestTimestamp = make([]int64, len(arg.AppID))
	}
	for i := range arg.AppID {
		in, err = r.Fetch(arg.Zone, arg.Env, arg.AppID[i], arg.LatestTimestamp[i], model.InstanceStatusUP)
		if err == ecode.NothingFound {
			miss = arg.AppID[i]
			log.Error("Polls zone(%s) env(%s) appid(%s) error(%v)", arg.Zone, arg.Env, arg.AppID[i], err)
			return
		}
		if err == nil {
			ins[arg.AppID[i]] = in
			new = true
		}
	}
	if new {
		ch = make(chan map[string]*model.InstanceInfo, 1)
		ch <- ins
		return
	}
	r.cLock.Lock()
	for i := range arg.AppID {
		k := pollKey(arg.Env, arg.AppID[i])
		if _, ok := r.conns[k]; !ok {
			r.conns[k] = make(map[string]*conn, 1)
		}
		connection, ok := r.conns[k][arg.Hostname]
		if !ok {
			if ch == nil {
				ch = make(chan map[string]*model.InstanceInfo, 5) // NOTE: there maybe have more than one connection on the same hostname!!!
			}
			connection = newConn(ch, arg.LatestTimestamp[i], arg)
			log.Info("Polls from(%s) new connection(%d)", arg.Hostname, connection.count)
		} else {
			connection.count++ // NOTE: there maybe have more than one connection on the same hostname!!!
			if ch == nil {
				ch = connection.ch
			}
			log.Info("Polls from(%s) reuse connection(%d)", arg.Hostname, connection.count)
		}
		r.conns[k][arg.Hostname] = connection
	}
	r.cLock.Unlock()
	return
}

// 清空已有的 poll chan， 并对已有的 poll chan 进行广播，将全新的 instanceInfo 加入 chan
// broadcast on poll by chan.
// NOTE: make sure free poll before update appid latest timestamp.
func (r *Registry) broadcast(env, appid string) {
	key := pollKey(env, appid)
	r.cLock.Lock()
	defer r.cLock.Unlock()
	// 获取当前 DeployEnv 下该 app 的所有 poll 请求的 chan
	conns, ok := r.conns[key]
	if !ok {
		return
	}
	delete(r.conns, key)
	for _, conn := range conns {
		ii, err := r.Fetch(conn.arg.Zone, env, appid, 0, model.InstanceStatusUP) // TODO(felix): latesttime!=0 increase
		if err != nil {
			// may be not found ,just continue until next poll return err.
			log.Error("get appid:%s env:%s zone:%s err:%v", appid, env, conn.arg.Zone, err)
			continue
		}
		for i := 0; i < conn.count; i++ {
			select {
			case conn.ch <- map[string]*model.InstanceInfo{appid: ii}: // NOTE: if chan is full, means no poller.
				log.Info("broadcast to(%s) success(%d)", conn.arg.Hostname, i+1)
			case <-time.After(time.Millisecond * 500):
				log.Info("broadcast to(%s) failed(%d) maybe chan full", conn.arg.Hostname, i+1)
			}
		}
	}
}

func pollKey(env, appid string) string {
	return fmt.Sprintf("%s.%s", env, appid)
}

// Set Set the metadata  of instance by hostnames.
func (r *Registry) Set(arg *model.ArgSet) (ok bool) {
	a, _, _ := r.apps(arg.AppID, arg.Env, arg.Zone)
	if len(a) == 0 {
		return
	}
	if ok = a[0].Set(arg); !ok {
		return
	}
	r.broadcast(arg.Env, arg.AppID)
	return
}

// 获取所有的微服务 app
func (r *Registry) allApp() (ass []*model.Apps) {
	r.aLock.RLock()
	ass = make([]*model.Apps, 0, len(r.appm))
	for _, as := range r.appm {
		ass = append(ass, as)
	}
	r.aLock.RUnlock()
	return
}

// todo (spell)
// reset expect renews, count the renew of all app, one app has two expect renews in minute.
func (r *Registry) resetExp() {
	cnt := int64(0)
	for _, p := range r.allApp() {
		for _, a := range p.App("") {
			cnt += int64(a.Len())
		}
	}
	r.gd.setExp(cnt)
}

func (r *Registry) proc() {
	tk := time.Tick(1 * time.Minute)
	tk2 := time.Tick(15 * time.Minute)
	for {
		select {
		case <-tk:
			// 每分钟更新一次心跳成功的数量
			r.gd.updateFac()
			r.evict()
		case <-tk2:
			// todo 为什么不每分钟重置一次
			// 每隔十五分钟，重新计算期望的心跳数
			r.resetExp()
		}
	}
}

// 踢出, https://www.infoq.cn/article/jlDJQ*3wtN2PcqTDyokh
// 只有大于阈值， 才会进入剔除
func (r *Registry) evict() {
	protect := r.gd.ok() // true: 心跳数小于预期
	// 先收集所有过期的实例，随机的把他们踢出。如果不这么做，对于较大的过期实例集合，可能会在自我保护开始前踢出所有应用。
	// 通过随机化，影响应该在所有应用程序中均匀分布

	// We collect first all expired instances, to evict them in random order. For large eviction sets,
	// if we do not that, we might wipe out whole apps before self preservation kicks in. By randomizing it,
	// the impact should be evenly distributed across all applications.
	var eis []*model.Instance
	var registrySize int
	// todo （change name）
	// all projects in current DeployEnv
	ass := r.allApp()
	for _, as := range ass {
		for _, a := range as.App("") {
			registrySize += a.Len()
			is := a.Instances()
			for _, i := range is {
				delta := time.Now().UnixNano() - i.RenewTimestamp
				// 1. 实际心跳次数大于期望数（认为是 discovery client 错误）， 心跳间隔大于 90 秒的，进入剔除列表
				// 2. 实际心跳小于期望数（认为是 discovery server 错误），心跳间隔大于 1 小时，则加入到剔除列表
				if (!protect && delta > _evictThreshold) || delta > _evictCeiling {
					eis = append(eis, i)
				}
			}
		}
	}

	// 为了补偿GC暂停或者本地时间漂移，需要使用当前注册了的所有的app的数量作为 自我保护机制的出发条件，如果不这样处理，可能会踢出所有 app
	// To compensate for GC pauses or drifting local time, we need to use current registry size as a base for
	// triggering self-preservation. Without that we would wipe out full registry.
	// 这里是期望踢出操作结束后，已注册的app不要小于原有的 85%
	eCnt := len(eis)
	registrySizeThreshold := int(float64(registrySize) * _percentThreshold)
	evictionLimit := registrySize - registrySizeThreshold
	if eCnt > evictionLimit {
		eCnt = evictionLimit
	}
	if eCnt == 0 {
		return
	}

	for i := 0; i < eCnt; i++ {
		// Pick a random item (Knuth shuffle algorithm)
		next := i + rand.Intn(len(eis)-i)
		eis[i], eis[next] = eis[next], eis[i]
		ei := eis[i]
		r.cancel(ei.Zone, ei.Env, ei.AppID, ei.Hostname, time.Now().UnixNano())
	}
}

// DelConns delete conn of host in appid
func (r *Registry) DelConns(arg *model.ArgPolls) {
	r.cLock.Lock()
	for i := range arg.AppID {
		k := pollKey(arg.Env, arg.AppID[i])
		conns, ok := r.conns[k]
		if !ok {
			log.Warn("DelConn key(%s) not found", k)
			continue
		}
		if connection, ok := conns[arg.Hostname]; ok {
			if connection.count > 1 {
				log.Info("DelConns from(%s) count decr(%d)", arg.Hostname, connection.count)
				connection.count--
			} else {
				log.Info("DelConns from(%s) delete(%d)", arg.Hostname, connection.count)
				delete(conns, arg.Hostname)
			}
		}
	}
	r.cLock.Unlock()
}
