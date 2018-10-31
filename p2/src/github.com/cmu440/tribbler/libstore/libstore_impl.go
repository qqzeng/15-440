package libstore

import (
	"container/list"
	"errors"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

const (
	maxRetryCnt   = 5
	retryInterval = 500
	TickInterval  = 1000
)

type libstore struct {
	// TODO: implement this!
	rwmu              sync.RWMutex           // guards read and write for hash table
	mu                sync.Mutex             // lock for normal case
	masterPeer        *rpc.Client            // storage master's rpc hub for slaves send rpc request.
	peers             map[string]*rpc.Client // storage peers for rpc, hostport => *rpc.Client, including the master
	cacheht           map[string]interface{} // cached hash table
	nodes             []storagerpc.Node      // all storage servers info, Node{nodeID, hostport}
	leaseMode         LeaseMode              // mode determines how the Libstore should request/handle leases
	hp                string                 // this Libstore's host:port for storage servers to call back notifications
	cacheLiveDuration map[string]int         // a map records cache live duration for keys
	cacheTicker       *time.Ticker           // for update key cache regularly
	sortedNodeIds     []uint32               // sorted nodeIDs to support consistent hash
	keyQueryStats     map[string][]time.Time // key queried stats to judge whether the key should be cached in libstore. key => list[queryTime0, ... ,queryTime4]
	keyWantLease      map[string]bool        // record whether key should request lease
	wantLeaseTicker   *time.Ticker           // for update keyWantLease
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	ls := &libstore{
		peers:             make(map[string]*rpc.Client),
		cacheht:           make(map[string]interface{}),
		leaseMode:         mode,
		hp:                myHostPort,
		cacheLiveDuration: make(map[string]int),
		cacheTicker:       time.NewTicker(TickInterval * time.Millisecond),
		keyQueryStats:     make(map[string][]time.Time),
		keyWantLease:      make(map[string]bool),
		wantLeaseTicker:   time.NewTicker(TickInterval * time.Millisecond),
	}
	// get storage serverList by GetServerList RPC.
	if err := ls.getSrvsFromStorageSrvMaster(masterServerHostPort); err != nil {
		return nil, err
	}
	// register LeaseCallbacks for storage servers to revoke leases.
	if err := rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls)); err != nil {
		return nil, err
	}
	go ls.updateCacheDurationRegularly()
	go ls.updateKeyWantLeaseRegularly()
	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	// return directly if the key cached in libstore.
	ls.rwmu.RLock()
	if v, exist := ls.cacheht[key]; exist {
		ls.rwmu.RUnlock()
		return v, nil
	}
	ls.rwmu.RUnlock()
	// select route nodeID for the key.
	nodeID := ls.routeNode(key)
	// send Get RPC to storage master and cache result.
	resVal, err := ls.sendGetRPCAndCacheResult(key, nodeID, "Get")
	return resVal, err
}

func (ls *libstore) Put(key, value string) error {
	var reply storagerpc.PutReply
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}
	nodeID := ls.routeNode(key)
	if err := ls.peers[nodeID].Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}
	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("Storage Server(%v) Put (%v => %v) error"), nodeID, key, value)
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	ls.rwmu.RLock()
	if v, exist := ls.cacheht[key]; exist {
		ls.rwmu.RUnlock()
		return v, nil
	}
	ls.rwmu.RUnlock()
	nodeID := ls.routeNode(key)
	resVal, err := ls.sendGetRPCAndCacheResult(key, nodeID, "GetList")
	return resVal, err
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	var reply storagerpc.PutReply
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}
	nodeID := ls.routeNode(key)
	if err := ls.peers[nodeID].Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.ItemNotFound {
		return errors.New(fmt.Sprintf("Storage server(%v) response ItemNotFound for (%v => %v).", nodeID, key, removeItem))
	}
	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("Storage server(%v) response RemoveFromList fail for (%v => %v).", nodeID, key, removeItem))
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	var reply storagerpc.PutReply
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: newItem,
	}
	nodeID := ls.routeNode(key)
	if err := ls.peers[nodeID].Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.ItemExists {
		return errors.New(fmt.Sprintf("Storage server(%v) response ItemExists for (%v => %v).", nodeID, key, newItem))
	}
	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("Storage server(%v) response AppendToList fail for (%v => %v)", nodeID, key, newItem))
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.rwmu.Lock()
	defer ls.rwmu.Unlock()
	v, exist := ls.cacheht[args.Key]
	if !exist {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
	delete(ls.cacheht, v)        // remove cached <key, value>
	ls.keyWantLease[key] = false // set want lease to false.
	// TODO: should remove queried info about the key.
	reply.Status = storagerpc.OK
	return nil
}

func (ls *libstore) getSrvsFromStorageSrvMaster(mshp string) error {
	p, err := rpc.DialHTTP("tcp", mshp)
	if err != nil {
		return err
	}
	ls.masterPeer = p
	ls.peers[mshp] = p // cache master connection
	// libstore(tribserver) sends getServerList rpc to storageServer master to get all storage servers.
	var reply storagerpc.GetServersReply
	args := &storagerpc.GetServersArgs{}
	retryCnt := 1
	for retryCnt <= maxRetryCnt {
		err := ls.masterPeer.Call("StorageServer.GetServers", args, &reply)
		if err != nil {
			return err
		}
		if reply.Status == storagerpc.OK {
			ls.nodes = reply.Servers
			// for _, node = range ls.nodes {
			// 	ls.peers[node.NodeID] = node
			// }
			return nil
		}
		time.Sleep(retryInterval * time.Millisecond) // not ready, sleep
		retryCnt++
	}
	return errors.New("Retry send GetServerList RPC to storage master for max times.")
}

// route key stored node using consistent hash ring.
func (ls *libstore) routeNode(key string) uint32 {
	tuple := strings.Split(key, ":")
	slotNo := libstore.StoreHash(tuple[0])
	if len(ls.sortedNodeIds) == 0 {
		for _, node = range ls.nodes {
			ls.sortedNodeIds = append(ls.sortedNodeIds, node.NodeID)
		}
		sort.Slice(ls.sortedNodeIds, func(i, j int) bool { return ls.sortedNodeIds[i] < ls.sortedNodeIds[j] })
	}
	// use binary search to find the right nodeID.
	idx := sort.Search(len(ls.sortedNodeIds), func(i int) bool { return ls.sortedNodeIds[i] >= slotNo })
	if idx == len(ls.sortedNodeIds) {
		idx = 0
	}
	return ls.sortedNodeIds[idx]
}

func (ls *libstore) sendGetRPCAndCacheResult(key string, nodeID uint32, methodCall string) (string, error) {
	resValue := ""
	ls.rwmu.Lock()
	if p, exist := ls.peers[nodeID]; !exit {
		p, err := rpc.DialHTTP("tcp", mshp)
		if err != nil {
			return resValue, err
		}
		ls.peers[nodeID] = p // cache storage peer's rpc connection.
	}
	ls.rwmu.Unlock()
	var reply storagerpc.GetReply
	args := &storagerpc.GetArgs{Key: key, HostPort: ls.hp}
	switch ls.leaseMode {
	case libstore.Never:
		args.WantLease = false
	case libstore.Always:
		args.WantLease = true
	case libstore.Normal:
		ls.rwmu.RLock()
		args.WantLease = false
		if want, ok := ls.keyWantLease[key]; ok {
			args.WantLease = want
		}
		ls.rwmu.RUnlock()
	default:
		return resValue, errors.New(fmt.Sprintf("Invalid lease mode %v.", ls.leaseMode))
	}
	err := ls.peers[nodeID].Call("StorageServer."+methodCall, args, &reply)
	if err != nil {
		return resValue, err
	}
	if reply.Status == storagerpc.KeyNotFound {
		return resValue, errors.New(fmt.Sprintf("Storage server(%v) response KeyNotFound for %v.", nodeID, key))
	}
	if args.WantLease {
		if reply.Lease == nil || reply.Lease.Granted {
			return resValue, errors.New(fmt.Sprintf("Storage master(%v) fail to grant lease for %v.", nodeID, key))
		}
	}
	ls.rwmu.Lock()
	defer ls.rwmu.Unlock()
	if args.WantLease {
		ls.cacheht[key] = reply.Value                            // cache value.
		ls.cacheLiveDuration = reply.Lease.ValidSeconds          // set expieration
		if len(ls.keyQueryStats) == storgerpc.QueryCacheThresh { // update key query records.
			for i := 1; i < storgerpc.QueryCacheThresh; i++ {
				ls.keyQueryStats[i-1] = ls.keyQueryStats[i]
			}
		}
		ls.keyQueryStats = append(ls.keyQueryStats, time.Now())
	}
	resValue = reply.Value
	return resValue, nil
}

func (ls *libstore) updateCacheDurationRegularly() {
	for {
		select {
		case <-ls.cacheTicker:
			ls.rwmu.Lock()
			for key, ld = range ls.cacheLiveDuration {
				ls.cacheLiveDuration[key] = ld - 1
				if ls.cacheLiveDuration[key] <= 0 {
					delete(ss.leaseDuration, key)
				}
			}
			ss.rwmu.Unlock()
		}
	}
}

func (ls *libstore) updateKeyWantLeaseRegularly() {
	for {
		select {
		case <-ls.wantLeaseTicker:
			ls.rwmu.Lock()
			for key, queryTimeWindow = range ls.keyQueryStats { // only update keys which has been queried.
				if queryTimeWindow == nil || len(queryTimeWindow) < storgerpc.QueryCacheThresh {
					ls.keyWantLease[key] = false
					continue
				}
				if time.Now().Sub(queryTimeWindow[0]) > storgerpc.QueryCacheSeconds {
					ls.keyWantLease[key] = false
					continue
				}
				ls.keyWantLease[key] = true
			}
			ls.rwmu.Unlock()
		}
	}
}
