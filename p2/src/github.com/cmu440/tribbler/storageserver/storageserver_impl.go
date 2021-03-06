package storageserver

import (
	"errors"
	"fmt"
	"log"
	"net"
	http "net/http"
	rpc "net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

const (
	chanSizeUnit = 1
	TickInterval = 1000
)

var (
	logger *log.Logger
)

type storageServer struct {
	// TODO: implement this!
	rwmu              sync.RWMutex              // guards read and write for hash table
	mu                sync.Mutex                // lock for normal case
	peerMutex         sync.Mutex                // guards client rpc handler
	leaseMutex        sync.Mutex                // guards lease map
	keysMutex         map[string]*sync.Mutex    // guards safely access the specific key, also imporve performance.
	ht                map[string]interface{}    // hash table for storage, the value type is either string or string array.
	nodes             []storagerpc.Node         // all storage servers info, Node{nodeID, hostport}
	peers             map[string]*rpc.Client    // peers for rpc, hostport => *rpc.Client, including the master
	masterPeer        *rpc.Client               // master's rpc hub for slaves send rpc request.
	connSrvs          map[uint32]bool           // connected servers only for master. nodeID => isConnected
	listener          net.Listener              // listener for listening rpc requests.
	nodeCnt           int                       // number of slaves
	nodeID            uint32                    // node id of current server
	initStartComplete chan bool                 // signal for all peers have joined the consistent hash ring.
	sortedNodeIds     []uint32                  // sorted key
	leaseDuration     map[string]map[string]int // lease duration for <key, node>: key => hostport array => liveDuration (second)
	leaseTicker       *time.Ticker              // update lease live duration for every one second.
	// nodes      map[uint32]storagerpc.Node // including storage server id and its host:port address, nodeID => Node{nodeID, hostport}
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	ss := &storageServer{
		keysMutex:         make(map[string]*sync.Mutex),
		ht:                make(map[string]interface{}),
		peers:             make(map[string]*rpc.Client),
		connSrvs:          make(map[uint32]bool),
		nodeCnt:           numNodes,
		nodeID:            nodeID,
		initStartComplete: make(chan bool, chanSizeUnit),
		leaseDuration:     make(map[string]map[string]int),
		leaseTicker:       time.NewTicker(TickInterval * time.Millisecond),
	}
	serverLogFile, _ := os.OpenFile("log_storage."+fmt.Sprintf("%d", port), os.O_RDWR|os.O_CREATE, 0666)
	logger = log.New(serverLogFile, "#[Storage] ", log.Lmicroseconds|log.Lshortfile)
	if err := ss.buildRPCListen(port); err != nil {
		return nil, err
	}
	node := storagerpc.Node{
		NodeID:   nodeID,
		HostPort: fmt.Sprintf("localhost:%d", port),
	}
	go ss.updateLeaseDurationRegularly() // update leases live duration regularly.
	if masterServerHostPort == "" {      // master first saves info for itself
		ss.nodes = append(ss.nodes, node)
		ss.connSrvs[nodeID] = true
	} else { // slave joins consisent hash ring
		if err := ss.joinHashRing(masterServerHostPort, node); err != nil {
			return nil, err
		}
	}
	if masterServerHostPort == "" {
		if ss.nodeCnt > 1 {
			logger.Printf("Storage master(%v) wait for slaves joining...\n", nodeID)
			select {
			case <-ss.initStartComplete: // wait for all nodes join
				logger.Printf("Storage master(%v) complete starting.\n", nodeID)
				// return ss, nil
			}
		}
		// else {
		//  return ss, nil // only one server, i.e. the master.
		// }
	}
	return ss, nil
}

func (ss *storageServer) buildRPCListen(port int) error {
	// master register itself to listen connections from other nodes.
	var err error
	var l net.Listener
	l, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Println("Master failed to listen: ", err)
		return err
	}
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	if err != nil {
		return err
	}
	ss.listener = l
	rpc.HandleHTTP()
	go http.Serve(ss.listener, nil)
	return nil
}

func (ss *storageServer) joinHashRing(masterServerHostPort string, node storagerpc.Node) error {
	logger.Printf("slave(%v) begin to join hash ring.\n", ss.nodeID)
	p, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return err
	}
	ss.masterPeer = p
	// ss.peers[masterServerHostPort] = p
	// slave sends register rpc to master.
	var reply storagerpc.RegisterReply
	args := &storagerpc.RegisterArgs{ServerInfo: node}
	ready := false // TODO: may setup a timer
	for !ready {
		err := ss.masterPeer.Call("StorageServer.RegisterServer", args, &reply)
		if err != nil {
			return err
		}
		if reply.Status == storagerpc.OK {
			ss.nodes = reply.Servers
			logger.Printf("slave(%v) successfully join hash ring.\n", ss.nodeID)
			return nil
		}
		time.Sleep(time.Millisecond * 1000) // not ready, sleep
	}
	return errors.New("Can not contact master.")
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.rwmu.Lock()
	defer ss.rwmu.Unlock()
	node := args.ServerInfo
	if _, joined := ss.connSrvs[node.NodeID]; !joined {
		ss.nodes = append(ss.nodes, node)
		ss.connSrvs[node.NodeID] = true
		logger.Printf("node(%v) has joined the hash ring, current joined nodes number is %v.\n", node.NodeID, len(ss.nodes))
		if len(ss.nodes) == ss.nodeCnt {
			logger.Printf("all nodes have joined the hash ring.\n")
			ss.initStartComplete <- true
		}
	}
	if len(ss.connSrvs) == ss.nodeCnt {
		reply.Status = storagerpc.OK
		reply.Servers = ss.nodes
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.rwmu.Lock()
	defer ss.rwmu.Unlock()
	logger.Printf("libstore GetServers from stroage server(%v), len(Servers)=%v.\n", ss.nodeID, len(ss.nodes))
	if len(ss.nodes) == ss.nodeCnt {
		reply.Status = storagerpc.OK
		reply.Servers = ss.nodes
		logger.Printf("libstore successfully GetServers from stroage server(%v), len(Servers)=%v.\n", ss.nodeID, len(ss.nodes))
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	logger.Printf("libstore begin to get key=%v from stroage server(%v).\n", args.Key, ss.nodeID)
	// check whether the key belongs to the storage range of this peer
	if right := ss.checkKeyRoute(args.Key); !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	km := ss.getKeyMutex(args.Key)
	km.Lock()
	defer km.Unlock()
	v, ok := ss.ht[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = v.(string)
		if args.WantLease {
			ss.setLeaseDurationForKeyNodePair(args.Key, args.HostPort, storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds)
		}
		reply.Lease = storagerpc.Lease{
			Granted:      args.WantLease,
			ValidSeconds: storagerpc.LeaseSeconds,
		}
	}
	logger.Printf("libstore successfully get <%v,%v> from stroage server(%v).\n", args.Key, reply.Value, ss.nodeID)
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	// check whether the key belongs to the storage range of this peer
	if right := ss.checkKeyRoute(args.Key); !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	km := ss.getKeyMutex(args.Key)
	km.Lock()
	defer km.Unlock()
	v, ok := ss.ht[args.Key]
	if !ok { //|| len(v.([]string)) == 0
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = v.([]string)
		if args.WantLease {
			ss.setLeaseDurationForKeyNodePair(args.Key, args.HostPort, storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds)
		}
		reply.Lease = storagerpc.Lease{
			Granted:      args.WantLease,
			ValidSeconds: storagerpc.LeaseSeconds,
		}
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	logger.Printf("libstore begin to put <%v,%v> to stroage server(%v).\n", args.Key, args.Value, ss.nodeID)
	if right := ss.checkKeyRoute(args.Key); !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	km := ss.getKeyMutex(args.Key) // using the assoicated mutex to block all node write (or any further leases) for that key
	km.Lock()
	defer km.Unlock()
	// 1. send revokeLease to all hostports which have been granted a lease(not exprire) for the key.
	// 2. wait all response, and resume release for the key until all hostports response ok.
	// 3. at the same time, check whether the key lease expires for every 500 milliseconds.
	// 1.1 get all unexpired nodes for the key.
	ss.waitAllKeyLeaseExpired(args.Key)
	ss.ht[args.Key] = args.Value
	reply.Status = storagerpc.OK
	logger.Printf("libstore successfully put <%v,%v> to stroage server(%v).\n", args.Key, args.Value, ss.nodeID)
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if right := ss.checkKeyRoute(args.Key); !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	km := ss.getKeyMutex(args.Key)
	km.Lock()
	defer km.Unlock()
	ss.waitAllKeyLeaseExpired(args.Key)
	// check whether the appended value is duplicate.
	values, exit := ss.ht[args.Key]
	var vlist []string
	if exit {
		vlist = values.([]string)
		for _, v := range vlist {
			if v == args.Value {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}
	}
	vlist = append(vlist, args.Value)
	ss.ht[args.Key] = vlist
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if right := ss.checkKeyRoute(args.Key); !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	km := ss.getKeyMutex(args.Key)
	km.Lock()
	defer km.Unlock()
	ss.waitAllKeyLeaseExpired(args.Key)
	values, exit := ss.ht[args.Key]
	var vlist []string
	if exit {
		vlist = values.([]string)
		for i, v := range vlist {
			if v == args.Value {
				reply.Status = storagerpc.OK
				if len(vlist) == 1 { // if empty, delete the slot.
					delete(ss.ht, args.Key)
					return nil
				}
				ss.ht[args.Key] = append(vlist[:i], vlist[i+1:len(vlist)]...)
				return nil
			}
		}
	}
	reply.Status = storagerpc.ItemNotFound
	return nil
}

// route key stored node using consistent hash ring.
func (ss *storageServer) checkKeyRoute(key string) bool {
	partitionKeys := strings.Split(key, ":")
	slotNo := libstore.StoreHash(partitionKeys[0])
	if len(ss.sortedNodeIds) == 0 {
		for _, node := range ss.nodes {
			ss.sortedNodeIds = append(ss.sortedNodeIds, node.NodeID)
		}
		sort.Slice(ss.sortedNodeIds, func(i, j int) bool { return ss.sortedNodeIds[i] < ss.sortedNodeIds[j] })
	}
	// use binary search to find the right nodeID.
	idx := sort.Search(len(ss.sortedNodeIds), func(i int) bool { return ss.sortedNodeIds[i] >= slotNo })
	if idx == len(ss.sortedNodeIds) {
		idx = 0
	}
	return ss.nodeID == ss.sortedNodeIds[idx]
}

func (ss *storageServer) getKeyMutex(key string) *sync.Mutex {
	ss.rwmu.Lock()
	defer ss.rwmu.Unlock()
	if _, ok := ss.keysMutex[key]; !ok {
		ss.keysMutex[key] = new(sync.Mutex)
	}
	return ss.keysMutex[key]
}

func (ss *storageServer) setLeaseDurationForKeyNodePair(key string, hostport string, liveDuration int) {
	ss.rwmu.Lock()
	defer ss.rwmu.Unlock()
	if _, ok := ss.leaseDuration[key]; !ok {
		ss.leaseDuration[key] = make(map[string]int)
	}
	ss.leaseDuration[key][hostport] = liveDuration
}

func (ss *storageServer) queryleaseDuration(key, addr string) int {
	defer ss.leaseMutex.Unlock()
	ss.leaseMutex.Lock()
	if _, ok := ss.leaseDuration[key]; !ok {
		return 0
	}
	if _, ok := ss.leaseDuration[key][addr]; !ok {
		return 0
	}
	return ss.leaseDuration[key][addr]
}

func (ss *storageServer) getAllUnexpiredNodes(key string) []string {
	var hps []string
	nodes, ok := ss.leaseDuration[key]
	if !ok {
		return hps
	}
	for hostport, ld := range nodes {
		if ld > 0 {
			hps = append(hps, hostport)
		}
	}
	return hps
}

func (ss *storageServer) sendRevokeReleaseRPC(key string, hp string, wg *sync.WaitGroup, revokeOk chan<- bool) {
	var p *rpc.Client
	var ok bool
	var err error
	ss.peerMutex.Lock()
	p, ok = ss.peers[hp]
	ss.peerMutex.Unlock()
	if !ok {
		p, err = rpc.DialHTTP("tcp", hp)
		if err != nil {
			return
		}
		ss.peerMutex.Lock()
		ss.peers[hp] = p
		ss.peerMutex.Unlock()
	}
	var reply storagerpc.RevokeLeaseReply
	args := &storagerpc.RevokeLeaseArgs{Key: key}
	err = p.Call("LeaseCallbacks.RevokeLease", args, &reply)
	if err != nil {
		return
	}
	// if reply.Status == storagerpc.OK || reply.Status == storagerpc.KeyNotFound {
	revokeOk <- true
	// }
}

func (ss *storageServer) invalidateLeaseForKeyNodePair(key, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	revokeOk := make(chan bool, chanSizeUnit)
	go ss.sendRevokeReleaseRPC(key, addr, wg, revokeOk)
	leaseTicker := time.NewTicker(time.Millisecond * 1000)
	for {
		select {
		case <-leaseTicker.C:
			if ss.queryleaseDuration(key, addr) == 0 {
				return
			}
		case <-revokeOk:
			ss.setLeaseDurationForKeyNodePair(key, addr, 0)
			return
		}
	}
}

func (ss *storageServer) waitAllKeyLeaseExpired(key string) {
	unexpiredNodes := ss.getAllUnexpiredNodes(key)
	var wg sync.WaitGroup
	for _, hp := range unexpiredNodes {
		wg.Add(1)
		// 1.2 for every unexpired <key, node>, invalidate the lease.
		go ss.invalidateLeaseForKeyNodePair(key, hp, &wg)
	}
	wg.Wait()
}

func (ss *storageServer) updateLeaseDurationRegularly() {
	for {
		select {
		case <-ss.leaseTicker.C:
			ss.leaseMutex.Lock()
			for key, hps := range ss.leaseDuration {
				for hp, ld := range hps {
					ss.leaseDuration[key][hp] = ld - 1
					if ss.leaseDuration[key][hp] <= 0 {
						delete(ss.leaseDuration[key], hp)
					}
				}
				if len(ss.leaseDuration[key]) == 0 {
					delete(ss.leaseDuration, key)
				}
			}
			ss.leaseMutex.Unlock()
		}
	}
}
