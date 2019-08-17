package discovery

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/bilibili/discovery/conf"
	"github.com/bilibili/discovery/model"
	"github.com/bilibili/discovery/registry"
	"github.com/bilibili/kratos/pkg/ecode"
	"github.com/bilibili/kratos/pkg/log"
)

var (
	_fetchAllURL = "http://%s/discovery/fetch/all"
)

// Protected return if service in init protect mode.
// if service in init protect mode,only support write,
// read operator isn't supported.
func (d *Discovery) Protected() bool {
	return d.protected
}

// syncUp populates the registry information from a peer eureka node.  // 同步其他对等节点的注册信息
func (d *Discovery) syncUp() {
	// 启动时配置文件中的 discovery 节点
	nodes := d.nodes.Load().(*registry.Nodes)
	for _, node := range nodes.AllNodes() {
		if nodes.Myself(node.Addr) {
			// 已有的节点
			continue
		}
		// 其他分区的节点
		uri := fmt.Sprintf(_fetchAllURL, node.Addr)
		var res struct {
			Code int                          `json:"code"`
			Data map[string][]*model.Instance `json:"data"`
		}
		if err := d.client.Get(context.TODO(), uri, "", nil, &res); err != nil {
			log.Error("d.client.Get(%v) error(%v)", uri, err)
			continue
		}
		if res.Code != 0 {
			log.Error("service syncup from(%s) failed ", uri)
			continue
		}
		// sync success from other node,exit protected mode  同步一个节点成功，退出保护模式
		d.protected = false
		for _, is := range res.Data {
			for _, i := range is {
				_ = d.registry.Register(i, i.LatestTimestamp)
			}
		}
		// NOTE: no return, make sure that all instances from other nodes register into self.
	}
	nodes.UP()
}

// regSelf 注册自己
func (d *Discovery) regSelf() context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	now := time.Now().UnixNano()
	ins := &model.Instance{
		Region:   d.c.Env.Region,
		Zone:     d.c.Env.Zone,
		Env:      d.c.Env.DeployEnv,
		Hostname: d.c.Env.Host,
		AppID:    model.AppID,
		Addrs: []string{
			"http://" + d.c.HTTPServer.Addr,
		},
		Status:          model.InstanceStatusUP,
		RegTimestamp:    now,
		UpTimestamp:     now,
		LatestTimestamp: now,
		RenewTimestamp:  now,
		DirtyTimestamp:  now,
	}
	// 注册
	d.Register(ctx, ins, now, false, false)
	// 注册后异步检查
	go func() {
		// 30 秒一次心跳检查
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				arg := &model.ArgRenew{
					AppID:    ins.AppID,
					Zone:     d.c.Env.Zone,
					Env:      d.c.Env.DeployEnv,
					Hostname: d.c.Env.Host,
				}
				if _, err := d.Renew(ctx, arg); err != nil && err == ecode.NothingFound {
					d.Register(ctx, ins, now, false, false)
				}
			case <-ctx.Done():
				// 退出时下线
				arg := &model.ArgCancel{
					AppID:    model.AppID,
					Zone:     d.c.Env.Zone,
					Env:      d.c.Env.DeployEnv,
					Hostname: d.c.Env.Host,
				}
				if err := d.Cancel(context.Background(), arg); err != nil {
					log.Error("d.Cancel(%+v) error(%v)", arg, err)
				}
				return
			}
		}
	}()
	return cancel
}

func (d *Discovery) nodesproc() {
	var (
		lastTs int64
	)
	for {
		arg := &model.ArgPolls{
			AppID:           []string{model.AppID},
			Env:             d.c.Env.DeployEnv,
			Hostname:        d.c.Env.Host,
			LatestTimestamp: []int64{lastTs},
		}
		ch, _, _, err := d.registry.Polls(arg)
		if err != nil && err != ecode.NotModified {
			log.Error("d.registry polls (%v) error(%v)", arg, err)
			time.Sleep(time.Second)
			continue
		}
		apps := <-ch
		// 从所有的服务中找出 AppID 对应的所有实例
		ins, ok := apps[model.AppID]
		if !ok || ins == nil {
			return
		}
		var (
			nodes []string
			zones = make(map[string][]string)
		)
		for _, ins := range ins.Instances {
			for _, in := range ins {
				for _, addr := range in.Addrs {
					u, err := url.Parse(addr)
					if err == nil && u.Scheme == "http" {
						if in.Zone == d.c.Env.Zone {
							nodes = append(nodes, u.Host)
						} else {
							zones[in.Zone] = append(zones[in.Zone], u.Host)
						}
					}
				}
			}
		}
		lastTs = ins.LatestTimestamp
		c := new(conf.Config)
		*c = *d.c
		c.Nodes = nodes
		c.Zones = zones
		ns := registry.NewNodes(c)
		ns.UP()
		d.nodes.Store(ns)
		log.Info("discovery changed nodes:%v zones:%v", nodes, zones)
	}
}
