package storageserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

const (
	chanSizeUint = 1
)

type storageServer struct {
	// TODO: implement this!
	rwmu              sync.RWMutex           // guards read and write for hash table
	mu                sync.Mutex             // lock for normal case
	keysMutex         map[string]*sync.Mutex // guards safely access the specific key, also imporve performance.
	ht                map[string]interface{} // hash table for storage, the value type is either string or string array.
	master            uint32                 // master's nodeID in storage cluster to identify the master
	nodes             []storagerpc.Node      // including storage server id and its host:port address, nodeID => Node{nodeID, hostport}
	peers             map[uint32]*rpc.Client // peers for rpc, nodeID => *rpc.Client, including the master
	masterPeer        *rpc.Client
	connSrvs          map[uint32]bool           // connected servers only for master. nodeID => isConnected
	listener          net.Listener              // listener for listening rpc requests.
	nodeCnt           int                       // number of slaves
	nodeID            uint32                    // node id of current server
	initStartComplete chan bool                 // signal for all peers have joined the consistent hash ring.
	sortedNodeIds     []uint32                  // sorted key
	leaseDuration     map[string]map[string]int // leaseTime for <key, node>: key => hostport array => liveDuration (second)

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
		ht:                make(map[string][]byte),
		peers:             make(map[uint32]*rpc.Client),
		connSrvs:          make(map[uint32]bool),
		nodeCnt:           numNodes,
		nodeID:            nodeID,
		initStartComplete: make(chan bool, chanSizeUint),
		leaseDuration:     make(map[string]map[string]int),
	}
	if err := ss.buildRPCListen(port); err != nil {
		return nil, err
	}
	if masterServerHostPort == "" { // master first saves info for itself
		node := &storagerpc.Node{
			NodeID:   nodeID,
			HostPort: fmt.Sprintf("localhost:%d", port),
		}
		ss.nodes = append(ss.nodes, node)
		ss.connSrvs[nodeID] = true
	} else { // slave joins consisent hash ring
		if err := ss.joinHashRing(masterServerHostPort); err != nil {
			return nil, err
		}
	}
	if masterServerHostPort == "" {
		if ss.nodeCnt > 1 {
			select {
			case <-ss.initStartComplete: // wait for all nodes join
				return ss, nil
			}
		} else {
			return ss, nil // only one server, the master.
		}
	}
	return ss, nil
}

func (ss *storageServer) buildRPCListen(port int) error {
	// master register itself to listen connections from other nodes.
	rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		fmt.Println("Master failed to listen: ", err)
	}
	ss.listener = l
	rpc.HandleHTTP()
	go http.Serve(ss.listener, nil)
	return nil
}

func (ss *storageServer) joinHashRing(masterServerHostPort string) error {
	p, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return err
	}
	ss.masterPeer = p
	// slave sends register rpc to master.
	var reply storagerpc.RegisterReply
	args := &storagerpc.RegisterArgs{ServerInfo: *ss.nodes[ss.nodeID]}
	ready := false // TODO: may setup a timer
	for !ready {
		err := ss.masterPeer.Call("StorageServer.RegisterServer", args, &reply)
		if err != nil {
			return err
		}
		if reply.Status == storagerpc.OK {
			ss.nodes = reply.Servers
			return nil
		}
		time.Sleep(time.Millisecond * 1000) // not ready, sleep
	}
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.rwmu.Lock()
	defer ss.rwmu.Unlock()
	node := args.ServerInfo
	if _, joined := ss.connSrvs[node.NodeID]; !joined {
		ss.nodes = append(ss.nodes, node)
		ss.connSrvs[node.nodeID] = true
		if len(ss.nodes) == ss.nodeCnt {
			ss.initStartComplete <- true
		}
	}
	if len(ss.connSrvs) == ss.nodeCnt {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if len(ss.nodes) == ss.nodeCnt {
		reply.Status = storagerpc.OK
		reply.Servers = ss.nodes
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	// check whether the key belongs to the storage range of this peer
	if right := ss.checkKeyRoute(key); !right {
		reply.Status = WrongServer
		return
	}
	km := ss.getKeyMutex(key)
	km.Lock()
	defer km.Unlock()
	v, ok := ss.ht[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = v.(string)
		if args.WantLease {
			ss.setLeaseTimeForKeyNodePair(args.Key, args.HostPort, storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds)
		}
		reply.Lease = storagerpc.Lease{
			Granted:      args.WantLease,
			ValidSeconds: storagerpc.LeaseSeconds,
		}
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	// check whether the key belongs to the storage range of this peer
	if right := ss.checkKeyRoute(key); !right {
		reply.Status = WrongServer
		return
	}
	km := ss.getKeyMutex(key)
	km.Lock()
	defer km.Unlock()
	v, ok := ss.ht[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = v.([]string)
		if args.WantLease {
			ss.setLeaseTimeForKeyNodePair(args.Key, args.HostPort, storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds)
		}
		reply.Lease = storagerpc.Lease{
			Granted:      args.WantLease,
			ValidSeconds: storagerpc.LeaseSeconds,
		}
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if right := ss.checkKeyRoute(key); !right {
		reply.Status = WrongServer
		return
	}
	km := ss.getKeyMutex(key)
	km.Lock()
	defer km.Unlock()
	ss.ht[args.Key] = args.Value
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) checkKeyRoute(key string) bool {
	partitionKeys := strings.Split(key, ":")
	slotNo := libstore.StoreHash(partitionKeys[0])
	if len(ss.sortedNodeIds) == 0 {
		for _, node = range ss.nodes {
			ss.sortedNodeIds = append(ss.sortedNodeIds, node.NodeID)
		}
		sort.Sort(ss.sortedNodeIds)
	}
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

func (ss *storageServer) setLeaseTimeForKeyNodePair(key string, hostport string, liveDuration int) {
	ss.rwmu.Lock()
	defer ss.rwmu.Unlock()
	if _, ok := ss.leaseDuration[key]; !ok {
		ss.leaseDuration[key] = make(map[string]int)
	}
	ss.leaseDuration[key][hostport] = liveDuration
}
