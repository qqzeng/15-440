package libstore

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	maxRetryCnt   = 5
	retryInterval = 1000
	TickInterval  = 1000
)

var (
	logger *log.Logger
)

type libstore struct {
	// TODO: implement this!
	rwmu              sync.RWMutex           // guards read and write for hash table
	mu                sync.Mutex             // lock for normal case
	masterPeer        *rpc.Client            // storage master's rpc hub for slaves send rpc request.
	peers             map[uint32]*rpc.Client // storage peers for rpc, hostport => *rpc.Client, including the master
	srvs              map[uint32]string      // hostport => nodeID
	cacheht           map[string]interface{} // cached hash table
	nodes             []storagerpc.Node      // all storage servers info, Node{nodeID, hostport}
	leaseMode         LeaseMode              // mode determines how the Libstore should request/handle leases
	hp                string                 // this Libstore's host:port for storage servers to call back notifications
	cacheLiveDuration map[string]int         // a map records cache live duration for keys
	cacheTicker       *time.Ticker           // for update key cache regularly
	sortedNodeIds     []uint32               // sorted nodeIDs to support consistent hash
	keyQueryStats     map[string][]time.Time // key queried stats to judge whether the key should be cached in libstore. key => list[queryTime0, ... ,queryTime4]
	keyWantLease      map[string]bool        // record whether key should request lease
	keywlMu           sync.RWMutex           // gurads for keyWantLease map access
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
		peers:             make(map[uint32]*rpc.Client),
		srvs:              make(map[uint32]string),
		cacheht:           make(map[string]interface{}),
		leaseMode:         mode,
		hp:                myHostPort,
		cacheLiveDuration: make(map[string]int),
		cacheTicker:       time.NewTicker(TickInterval * time.Millisecond),
		keyQueryStats:     make(map[string][]time.Time),
		keyWantLease:      make(map[string]bool),
		wantLeaseTicker:   time.NewTicker(TickInterval / 100 * time.Millisecond),
	}
	serverLogFile, _ := os.OpenFile("log_libstore", os.O_RDWR|os.O_CREATE, 0666)
	logger = log.New(serverLogFile, "[Libstore] ", log.Lmicroseconds|log.Lshortfile)
	if myHostPort == "" {
		ls.leaseMode = Never
	}
	// get storage serverList by GetServerList RPC.
	if err := ls.getSrvsFromStorageSrvMaster(masterServerHostPort); err != nil {
		return nil, err
	}
	// register LeaseCallbacks for storage servers to revoke leases.
	if err := rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls)); err != nil {
		return nil, err
	}
	// update cache lease duration regularly
	go ls.updateCacheDurationRegularly()
	// update query operation stats regularly
	// go ls.updateKeyWantLeaseRegularly()
	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	// return directly if the key cached in libstore.
	ls.rwmu.RLock()
	if v, exist := ls.cacheht[key]; exist {
		ls.rwmu.RUnlock()
		return v.(string), nil
	}
	ls.rwmu.RUnlock()
	// select route nodeID for the key.
	nodeID := ls.routeNode(key)
	logger.Printf("node begin to get  key=%v from stroage server(%v).\n", key, nodeID)
	// send Get RPC to storage master and cache result.
	res, err := ls.sendGetRPCAndCacheResult(key, nodeID, "Get")
	logger.Printf("node successfully get  <%v,%v> from stroage server(%v).\n", key, res, nodeID)
	return res, err
}

func (ls *libstore) Put(key, value string) error {
	var reply storagerpc.PutReply
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}
	nodeID := ls.routeNode(key)
	logger.Printf("node begin put <%v, %v> to stroage server(%v).\n", key, value, nodeID)
	if err := ls.peers[nodeID].Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}
	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("Storage Server(%v) Put (%v => %v) error", nodeID, key, value))
	}
	logger.Printf("node successfully put <%v, %v> to stroage server(%v).\n", key, value, nodeID)
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	ls.rwmu.RLock()
	if v, exist := ls.cacheht[key]; exist {
		ls.rwmu.RUnlock()
		return v.([]string), nil
	}
	ls.rwmu.RUnlock()
	nodeID := ls.routeNode(key)
	res, err := ls.sendGetListRPCAndCacheResult(key, nodeID, "GetList")
	return res, err
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
		return errors.New("ItemNotFound")
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
		return errors.New("ItemExists")
	}
	if reply.Status != storagerpc.OK {
		return errors.New(fmt.Sprintf("Storage server(%v) response AppendToList fail for (%v => %v)", nodeID, key, newItem))
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.rwmu.Lock()
	defer ls.rwmu.Unlock()
	_, exist := ls.cacheht[args.Key]
	if !exist {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
	delete(ls.cacheht, args.Key)      // remove cached <key, value>
	ls.keyWantLease[args.Key] = false // set want lease to false.
	// TODO: should remove queried info about the key.
	reply.Status = storagerpc.OK
	return nil
}

func (ls *libstore) getSrvsFromStorageSrvMaster(mshp string) error {
	logger.Printf("node begin to DialHTTP (%v).\n", mshp)
	p, err := rpc.DialHTTP("tcp", mshp)
	if err != nil {
		return err
	}
	ls.masterPeer = p
	logger.Printf("node successfully DialHTTP (%v).\n", mshp)
	// ls.peers[mshp] = p // cache master connection
	// libstore(tribserver) sends getServerList rpc to storageServer master to get all storage servers.
	var reply storagerpc.GetServersReply
	args := &storagerpc.GetServersArgs{}
	retryCnt := 1
	for retryCnt <= maxRetryCnt {
		logger.Printf("node begin to getSrvsFromStorageSrvMaster (%v).\n", mshp)
		err := ls.masterPeer.Call("StorageServer.GetServers", args, &reply)
		if err != nil {
			return err
		}
		if reply.Status == storagerpc.OK {
			ls.nodes = reply.Servers
			logger.Printf("node successfully getSrvsFromStorageSrvMaster (%v).\n", mshp)
			for _, node := range ls.nodes { // cache map: nodeID => hostport
				ls.srvs[node.NodeID] = node.HostPort
			}
			for _, node := range reply.Servers {
				if strings.EqualFold(node.HostPort, mshp) {
					ls.peers[node.NodeID] = ls.masterPeer
					ls.sortedNodeIds = append(ls.sortedNodeIds, node.NodeID)
					continue
				}
				logger.Printf("node begin to DialHTTP to (%v).\n", node.HostPort)
				p, err := rpc.DialHTTP("tcp", node.HostPort)
				if err != nil {
					fmt.Println(err.Error())
					return err
				}
				ls.peers[node.NodeID] = p
				ls.sortedNodeIds = append(ls.sortedNodeIds, node.NodeID)
				logger.Printf("node successfully DialHTTP to (%v).\n", node.HostPort)
			}
			sort.Slice(ls.sortedNodeIds, func(i, j int) bool { return ls.sortedNodeIds[i] < ls.sortedNodeIds[j] })
			logger.Printf("node successfully getSrvsFromStorageSrvMaster (%v), len(ls.peers)=%v.\n", mshp, len(ls.peers))
			// break
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
	slotNo := StoreHash(tuple[0])
	if len(ls.sortedNodeIds) == 0 {
		for _, node := range ls.nodes {
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
	var reply storagerpc.GetReply
	args := &storagerpc.GetArgs{Key: key}
	var err error
	if err = ls.decideLeaseMode(key, args); err != nil {
		return resValue, err
	}
	err = ls.peers[nodeID].Call("StorageServer."+methodCall, args, &reply)
	if err != nil {
		// there have been cases where the connection is strangely disconnected, so connect to server again.
		var p *rpc.Client
		if strings.EqualFold(err.Error(), "connection is shut down") {
			p, err = rpc.DialHTTP("tcp", ls.srvs[nodeID])
			if err != nil {
				return resValue, err
			}
			ls.peers[nodeID] = p
			err = ls.peers[nodeID].Call("StorageServer."+methodCall, args, &reply)
			if err != nil {
				return resValue, err
			}
		} else {
			return resValue, err
		}
		return resValue, err
	}
	// if reply.Status == storagerpc.KeyNotFound || reply.Value == "" {
	// if reply.Status == storagerpc.KeyNotFound {
	// 	return resValue, errors.New("KeyNotFound")
	// }
	if reply.Status != storagerpc.OK {
		return resValue, errors.New("Error Get  " + fmt.Sprintf("%s :  %v", key, reply.Status))
	}
	ls.rwmu.Lock()
	// if args.WantLease {
	if reply.Lease.Granted {
		ls.cacheht[key] = reply.Value                        // cache value.
		ls.cacheLiveDuration[key] = reply.Lease.ValidSeconds // set expieration
		// fmt.Printf("[Get] cached %v\n", key)
	}
	ls.updateQueryStats(key)
	ls.rwmu.Unlock()
	ls.updateKeyWantLeaseRegularly()
	resValue = reply.Value
	return resValue, nil
}

func (ls *libstore) sendGetListRPCAndCacheResult(key string, nodeID uint32, methodCall string) ([]string, error) {
	var resValue []string
	var reply storagerpc.GetListReply
	args := &storagerpc.GetArgs{Key: key}
	var err error
	if err = ls.decideLeaseMode(key, args); err != nil {
		return resValue, err
	}
	err = ls.peers[nodeID].Call("StorageServer."+methodCall, args, &reply)
	if err != nil {
		// there have been cases where the connection is strangely disconnected, so connect to server again.
		var p *rpc.Client
		if strings.EqualFold(err.Error(), "connection is shut down") {
			p, err = rpc.DialHTTP("tcp", ls.srvs[nodeID])
			if err != nil {
				return resValue, err
			}
			ls.peers[nodeID] = p
			err = ls.peers[nodeID].Call("StorageServer."+methodCall, args, &reply)
			if err != nil {
				fmt.Println("[sendGetListRPCAndCacheResult]: %v", err.Error())
				return resValue, err
			}
			fmt.Println("[sendGetListRPCAndCacheResult]")
		} else {
			return resValue, err
		}
		return resValue, err
	}
	// if reply.Status == storagerpc.KeyNotFound || len(reply.Value) == 0 {
	if reply.Status == storagerpc.KeyNotFound {
		// return resValue, errors.New("KeyNotFound")
		return resValue, nil
	}
	if reply.Status != storagerpc.OK {
		return nil, errors.New("Error GetList")
	}
	ls.rwmu.Lock()
	// if args.WantLease {
	if reply.Lease.Granted {
		ls.cacheht[key] = reply.Value                        // cache value.
		ls.cacheLiveDuration[key] = reply.Lease.ValidSeconds // set expieration
	}
	ls.updateQueryStats(key)
	ls.rwmu.Unlock()
	ls.updateKeyWantLeaseRegularly()
	resValue = reply.Value
	return resValue, nil
}

func (ls *libstore) updateQueryStats(key string) {
	queryWindow, _ := ls.keyQueryStats[key]
	if len(queryWindow) == storagerpc.QueryCacheThresh { // update key query records.
		for i := 1; i < storagerpc.QueryCacheThresh; i++ {
			queryWindow[i-1] = queryWindow[i]
		}
	}
	queryWindow = append(queryWindow, time.Now())
	ls.keyQueryStats[key] = queryWindow
}

func (ls *libstore) decideLeaseMode(key string, args *storagerpc.GetArgs) error {
	switch ls.leaseMode {
	case Never:
		args.WantLease = false
	case Always:
		args.WantLease = true
		args.HostPort = ls.hp
	case Normal:
		ls.keywlMu.RLock()
		args.WantLease = false
		if want, ok := ls.keyWantLease[key]; ok {
			args.WantLease = want
			if want {
				args.HostPort = ls.hp
			}
		}
		ls.keywlMu.RUnlock()
	default:
		return errors.New(fmt.Sprintf("Invalid lease mode %v.", ls.leaseMode))
	}
	return nil
}

func (ls *libstore) updateCacheDurationRegularly() {
	for {
		select {
		case <-ls.cacheTicker.C:
			ls.rwmu.Lock()
			for key, ld := range ls.cacheLiveDuration {
				ls.cacheLiveDuration[key] = ld - 1
				if ls.cacheLiveDuration[key] <= 0 {
					delete(ls.cacheLiveDuration, key)
					delete(ls.cacheht, key)
				}
			}
			ls.rwmu.Unlock()
		}
	}
}

func (ls *libstore) updateKeyWantLeaseRegularly() {
	ls.keywlMu.Lock()
	for key, queryTimeWindow := range ls.keyQueryStats { // only update keys which has been queried.
		if queryTimeWindow == nil || len(queryTimeWindow) < storagerpc.QueryCacheThresh {
			ls.keyWantLease[key] = false
			continue
		}
		if time.Now().Sub(queryTimeWindow[0]).Minutes() > storagerpc.QueryCacheSeconds {
			ls.keyWantLease[key] = false
			continue
		}
		ls.keyWantLease[key] = true
	}
	ls.keywlMu.Unlock()
}

// func (ls *libstore) updateKeyWantLeaseRegularly() {
//  for {
//      select {
//      case <-ls.wantLeaseTicker.C:
//          ls.keywlMu.Lock()
//          for key, queryTimeWindow := range ls.keyQueryStats { // only update keys which has been queried.
//              if queryTimeWindow == nil || len(queryTimeWindow) < storagerpc.QueryCacheThresh {
//                  ls.keyWantLease[key] = false
//                  continue
//              }
//              if time.Now().Sub(queryTimeWindow[0]) > storagerpc.QueryCacheSeconds {
//                  ls.keyWantLease[key] = false
//                  continue
//              }
//              ls.keyWantLease[key] = true
//          }
//          ls.keywlMu.Unlock()
//      }
//  }
// }
