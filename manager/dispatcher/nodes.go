package dispatcher

import (
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/Sirupsen/logrus"
	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/identity"
	"github.com/docker/swarmkit/manager/dispatcher/heartbeat"
)

const rateLimitCount = 3

type registeredNode struct {
	SessionID  string
	Heartbeat  *heartbeat.Heartbeat
	Registered time.Time
	Attempts   int
	Node       *api.Node
	Disconnect chan struct{} // signal to disconnect
	mu         sync.Mutex
}

// checkSessionID determines if the SessionID has changed and returns the
// appropriate GRPC error code.
//
// This may not belong here in the future.
func (rn *registeredNode) checkSessionID(sessionID string) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Before each message send, we need to check the nodes sessionID hasn't
	// changed. If it has, we will the stream and make the node
	// re-register.
	if sessionID == "" || rn.SessionID != sessionID {
		return grpc.Errorf(codes.InvalidArgument, ErrSessionInvalid.Error())
	}

	return nil
}

type nodeStore struct {
	periodChooser                *periodChooser
	gracePeriodMultiplierNormal  time.Duration
	gracePeriodMultiplierUnknown time.Duration
	rateLimitPeriod              time.Duration
	nodes                        map[string]*registeredNode
	nodesByZone                  map[string]map[string]bool
	mu                           sync.RWMutex
}

func newNodeStore(hbPeriod, hbEpsilon time.Duration, graceMultiplier int, rateLimitPeriod time.Duration) *nodeStore {
	return &nodeStore{
		nodes:                        make(map[string]*registeredNode),
		nodesByZone:                  make(map[string]map[string]bool),
		periodChooser:                newPeriodChooser(hbPeriod, hbEpsilon),
		gracePeriodMultiplierNormal:  time.Duration(graceMultiplier),
		gracePeriodMultiplierUnknown: time.Duration(graceMultiplier) * 2,
		rateLimitPeriod:              rateLimitPeriod,
	}
}

func (s *nodeStore) updatePeriod(hbPeriod, hbEpsilon time.Duration, gracePeriodMultiplier int) {
	s.mu.Lock()
	s.periodChooser = newPeriodChooser(hbPeriod, hbEpsilon)
	s.gracePeriodMultiplierNormal = time.Duration(gracePeriodMultiplier)
	s.gracePeriodMultiplierUnknown = s.gracePeriodMultiplierNormal * 2
	s.mu.Unlock()
}

func (s *nodeStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.nodes)
}

func (s *nodeStore) AddUnknown(n *api.Node, expireFunc func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rn := &registeredNode{
		Node: n,
	}
	s.nodes[n.ID] = rn
	logrus.Errorf("DEBUG zone is %s", n.Description.Zone)
	_, ok := s.nodesByZone[n.Description.Zone]
	if !ok {
		s.nodesByZone[n.Description.Zone] = make(map[string]bool)
	}
	s.nodesByZone[n.Description.Zone][n.ID] = true
	rn.Heartbeat = heartbeat.New(s.periodChooser.Choose()*s.gracePeriodMultiplierUnknown, expireFunc)
	return nil
}

// CheckRateLimit returns error if node with specified id is allowed to re-register
// again.
func (s *nodeStore) CheckRateLimit(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existRn, ok := s.nodes[id]; ok {
		if time.Since(existRn.Registered) > s.rateLimitPeriod {
			existRn.Attempts = 0
		}
		existRn.Attempts++
		if existRn.Attempts > rateLimitCount {
			return grpc.Errorf(codes.Unavailable, "node %s exceeded rate limit count of registrations", id)
		}
		existRn.Registered = time.Now()
	}
	return nil
}

// Add adds new node and returns it, it replaces existing without notification.
func (s *nodeStore) Add(n *api.Node, expireFunc func()) *registeredNode {
	s.mu.Lock()
	defer s.mu.Unlock()
	var attempts int
	var registered time.Time
	if existRn, ok := s.nodes[n.ID]; ok {
		attempts = existRn.Attempts
		registered = existRn.Registered
		existRn.Heartbeat.Stop()
		delete(s.nodes, n.ID)
	}
	if registered.IsZero() {
		registered = time.Now()
	}
	rn := &registeredNode{
		SessionID:  identity.NewID(), // session ID is local to the dispatcher.
		Node:       n,
		Registered: registered,
		Attempts:   attempts,
		Disconnect: make(chan struct{}),
	}
	s.nodes[n.ID] = rn
	// _, ok := s.nodesByZone[n.Description.Zone]
	// if !ok {
	// 	s.nodesByZone[n.Description.Zone] = make(map[string]*registeredNode)
	// }
	// s.nodesByZone[n.Description.Zone][n.ID] = rn
	rn.Heartbeat = heartbeat.New(s.periodChooser.Choose()*s.gracePeriodMultiplierNormal, expireFunc)
	return rn
}

func (s *nodeStore) Get(id string) (*registeredNode, error) {
	s.mu.RLock()
	rn, ok := s.nodes[id]
	s.mu.RUnlock()
	if !ok {
		return nil, grpc.Errorf(codes.NotFound, ErrNodeNotRegistered.Error())
	}
	return rn, nil
}

func (s *nodeStore) GetWithSession(id, sid string) (*registeredNode, error) {
	s.mu.RLock()
	rn, ok := s.nodes[id]
	s.mu.RUnlock()
	if !ok {
		return nil, grpc.Errorf(codes.NotFound, ErrNodeNotRegistered.Error())
	}
	return rn, rn.checkSessionID(sid)
}

func (s *nodeStore) AddZone(id, zone string) error {
	s.mu.RLock()
	_, ok := s.nodes[id]
	if !ok {
		s.mu.RUnlock()
		return ErrNodeNotRegistered
	}

	_, ok = s.nodesByZone[zone]
	if !ok {
		s.nodesByZone[zone] = make(map[string]bool)
	}
	s.nodesByZone[zone][id] = true
	s.mu.RUnlock()
	return nil
}

func (s *nodeStore) GetZoneNeighbors(zone string, limit int) ([]string, error) {
	s.mu.RLock()
	nodesZone, ok := s.nodesByZone[zone]
	if !ok {
		s.mu.RUnlock()
		return nil, grpc.Errorf(codes.NotFound, ErrNodeNotRegistered.Error())
	}
	if limit <= 0 {
		limit = len(nodesZone)
	}
	neighbors := make([]string, 0, limit)
	for nid := range nodesZone {
		neighbors = append(neighbors, nid)
		limit--
		if limit == 0 {
			break
		}
	}
	s.mu.RUnlock()
	return neighbors, nil
}

func (s *nodeStore) Heartbeat(id, sid string) (time.Duration, error) {
	rn, err := s.GetWithSession(id, sid)
	if err != nil {
		return 0, err
	}
	period := s.periodChooser.Choose() // base period for node
	grace := period * time.Duration(s.gracePeriodMultiplierNormal)
	rn.mu.Lock()
	rn.Heartbeat.Update(grace)
	rn.Heartbeat.Beat()
	rn.mu.Unlock()
	return period, nil
}

func (s *nodeStore) Delete(id string) *registeredNode {
	s.mu.Lock()
	var node *registeredNode
	if rn, ok := s.nodes[id]; ok {
		delete(s.nodesByZone[rn.Node.Description.Zone], id)
		delete(s.nodes, id)
		rn.Heartbeat.Stop()
		node = rn
	}
	s.mu.Unlock()
	return node
}

func (s *nodeStore) Disconnect(id string) {
	s.mu.Lock()
	if rn, ok := s.nodes[id]; ok {
		close(rn.Disconnect)
		rn.Heartbeat.Stop()
	}
	s.mu.Unlock()
}

// Clean removes all nodes and stops their heartbeats.
// It's equivalent to invalidate all sessions.
func (s *nodeStore) Clean() {
	s.mu.Lock()
	for _, rn := range s.nodes {
		rn.Heartbeat.Stop()
	}
	s.nodes = make(map[string]*registeredNode)
	s.nodesByZone = make(map[string]map[string]bool)
	s.mu.Unlock()
}
