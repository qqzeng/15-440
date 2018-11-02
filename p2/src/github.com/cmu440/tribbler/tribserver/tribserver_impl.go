package tribserver

import (
	"errors"

	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	maxRetivedTribbleCnt = 100
)

type tribServer struct {
	// TODO: implement this!
	rwmu       sync.RWMutex // guards read and write
	mu         sync.Mutex   // lock for normal case
	mshp       string       // master storage server's host:port
	myHostPort string
	listener   net.Listener      // listener for listening rpc requests.
	ls         libstore.Libstore // libstore instance to communicate with storage servers.
}

var (
	logger *log.Logger
)

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	ts := &tribServer{}
	// register and listen.
	var err error
	err = ts.buildRPCListen(myHostPort)
	if err != nil {
		return nil, err
	}
	// create libstore instance to communicate with storage servers.
	ts.ls, err = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	if err != nil {
		return nil, err
	}
	serverLogFile, _ := os.OpenFile("log_tribserver", os.O_RDWR|os.O_CREATE, 0666)
	logger = log.New(serverLogFile, "[Tribserver] ", log.Lmicroseconds|log.Lshortfile)
	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userID := args.UserID + ":" + "UserID" // for libstore to route storage node
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if _, err := ts.ls.Get(userID); err == nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	if err := ts.ls.Put(userID, ""); err != nil {
		return errors.New(fmt.Sprintf("Tribserver create user error: %v.", err.Error()))
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	userID := args.UserID + ":" + "UserID"
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if _, err := ts.ls.Get(userID); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	targetUserID := args.TargetUserID + ":" + "UserID"
	if _, err := ts.ls.Get(targetUserID); err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	userSubsKey := args.UserID + ":" + "subscriptionsList"
	if err := ts.ls.AppendToList(userSubsKey, args.TargetUserID); err != nil {
		if strings.EqualFold(err.Error(), "ItemExists") {
			reply.Status = tribrpc.Exists
			return nil
		}
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	userID := args.UserID + ":" + "UserID"
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if _, err := ts.ls.Get(userID); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	targetUserID := args.TargetUserID + ":" + "UserID"
	if _, err := ts.ls.Get(targetUserID); err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	userSubsKey := args.UserID + ":" + "subscriptionsList"
	if err := ts.ls.RemoveFromList(userSubsKey, args.TargetUserID); err != nil {
		if strings.EqualFold(err.Error(), "ItemNotFound") {
			reply.Status = tribrpc.NoSuchTargetUser
			return nil
		}
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	userID := args.UserID + ":" + "UserID"
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if _, err := ts.ls.Get(userID); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	userSubsKey := args.UserID + ":" + "subscriptionsList"
	var err error
	var userIDs []string
	userIDs, err = ts.ls.GetList(userSubsKey)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	reply.UserIDs = userIDs
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	userID := args.UserID + ":" + "UserID"
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if _, err := ts.ls.Get(userID); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	userTribsListKey := args.UserID + ":" + "TribblesList"
	// store tribbles effeciently
	userTribKey := fmt.Sprintf("%s:tribble %v %v", args.UserID, time.Now().UnixNano(), libstore.StoreHash(args.Contents))
	if err := ts.ls.AppendToList(userTribsListKey, userTribKey); err != nil {
		return err
	}
	if err := ts.ls.Put(userTribKey, args.Contents); err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userID := args.UserID + ":" + "UserID"
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if _, err := ts.ls.Get(userID); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	userTribsListKey := args.UserID + ":" + "TribblesList"
	var err error
	var tribs []string
	tribs, err = ts.ls.GetList(userTribsListKey)
	if err != nil {
		return err
	}
	var tribList []tribrpc.Tribble
	var tribContents string
	for i := len(tribs) - 1; i >= 0; i-- {
		// for i, tribKey := range tribs {
		tribKey := tribs[i]
		_, timestamp, _ := ts.parseUserTribsKey(tribKey)
		tribContents, err = ts.ls.Get(tribKey)
		if err != nil {
			return err
		}
		trib := tribrpc.Tribble{
			UserID:   args.UserID,
			Posted:   time.Unix(0, timestamp).UTC(),
			Contents: tribContents,
		}
		tribList = append(tribList, trib)
		if len(tribList) >= maxRetivedTribbleCnt {
			break
		}
	}
	reply.Tribbles = tribList
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userID := args.UserID + ":" + "UserID"
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if _, err := ts.ls.Get(userID); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	userSubsKey := args.UserID + ":" + "subscriptionsList"
	var err error
	var subs []string
	subs, err = ts.ls.GetList(userSubsKey)
	if err != nil {
		return err
	}
	var tribList []tribrpc.Tribble
	tribList, err = ts.getLatestTopNSubsTribs(subs)
	if err != nil {
		return err
	}
	reply.Tribbles = tribList
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) getLatestTopNSubsTribs(subs []string) ([]tribrpc.Tribble, error) {
	var tribStrListTotal []string
	var err error
	// get all users subscription
	for _, subUserID := range subs {
		userTribsListKey := subUserID + ":" + "TribblesList"
		var tribStrList []string
		tribStrList, err = ts.ls.GetList(userTribsListKey)
		if err != nil {
			return nil, err
		}
		if len(tribStrList) > maxRetivedTribbleCnt {
			tribStrList = tribStrList[len(tribStrList)-maxRetivedTribbleCnt:]
		}
		tribStrListTotal = append(tribStrListTotal, tribStrList...)
	}
	var tribList []tribrpc.Tribble
	var tribContents string
	// get all tribbles
	for _, tribKey := range tribStrListTotal {
		userID, timestamp, _ := ts.parseUserTribsKey(tribKey)
		tribContents, err = ts.ls.Get(tribKey)
		if err != nil {
			return nil, err
		}
		trib := tribrpc.Tribble{
			UserID:   userID,
			Posted:   time.Unix(0, timestamp).UTC(),
			Contents: tribContents,
		}
		tribList = append(tribList, trib)
	}
	sort.Slice(tribList[:], func(i, j int) bool {
		return tribList[i].Posted.UnixNano() > tribList[j].Posted.UnixNano()
	})
	if len(tribList) > maxRetivedTribbleCnt {
		tribList = tribList[:maxRetivedTribbleCnt]
	}
	return tribList, nil
}

func (ts *tribServer) buildRPCListen(myHostPort string) error {
	// master register itself to listen connections from other nodes.
	var err error
	var l net.Listener
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(ts))
	if err != nil {
		logger.Println("TribServer register name error: ", err)
		return err
	}
	l, err = net.Listen("tcp", myHostPort)
	if err != nil {
		logger.Println("TribServer failed to listen: ", err)
		return err
	}
	ts.listener = l
	rpc.HandleHTTP()
	go http.Serve(ts.listener, nil)
	return nil
}

func (ts *tribServer) parseUserTribsKey(tribKey string) (string, int64, uint64) {
	tmp := strings.Split(tribKey, ":")
	var timestamp int64
	var contentsHash uint64
	fmt.Sscanf(tmp[1], "tribble %v %v", &timestamp, &contentsHash)
	return tmp[0], timestamp, contentsHash
}
