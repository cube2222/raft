package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/cube2222/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type Cluster struct {
	cluster *serf.Serf

	connections      map[string]connection
	connectionsMutex sync.RWMutex
}

type connection struct {
	grpcConn *grpc.ClientConn
	term     int64
}

func NewCluster(localNodeName, clusterAddress string) (*Cluster, error) {
	cluster, err := setupCluster(
		localNodeName,
		clusterAddress,
	)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't setup cluster")
	}

	connections := make(map[string]connection)

	return &Cluster{
		cluster:     cluster,
		connections: connections,
	}, nil
}

func setupCluster(nodeName string, clusterAddr string) (*serf.Serf, error) {
	conf := serf.DefaultConfig()
	conf.Init()

	conf.MemberlistConfig.Name = nodeName

	cluster, err := serf.Create(conf)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't create cluster")
	}

	_, err = cluster.Join([]string{clusterAddr}, true)
	if err != nil {
		log.Printf("Couldn't join cluster, starting own: %v\n", err)
	}

	return cluster, nil
}

func (c *Cluster) NumMembers() int {
	return c.cluster.NumNodes()
}

func (c *Cluster) OtherMembers() []serf.Member {
	members := c.cluster.Members()
	for i, member := range members {
		if member.Name == c.cluster.LocalMember().Name {
			if i == len(members)-1 {
				members = members[:i]
			} else {
				members = append(members[:i], members[i+1:]...)
			}
			break
		}
	}
	return members
}

func (c *Cluster) OtherHealthyMembers() []serf.Member {
	members := c.OtherMembers()
	out := []serf.Member{}

	for _, member := range members {
		if member.Status == serf.StatusAlive {
			out = append(out, member)
		}
	}

	return out
}

func (c *Cluster) LocalNodeName() string {
	return c.cluster.LocalMember().Name
}

func (c *Cluster) Leave() error {
	return c.cluster.Leave()
}

func (c *Cluster) GetMember(memberName string) (*serf.Member, error) {
	for _, member := range c.OtherMembers() {
		if member.Name == memberName {
			return &member, nil
		}
	}

	return nil, errors.Errorf("Couldn't find member: %v", memberName)
}

func (c *Cluster) GetRaftConnection(ctx context.Context, term int64, member string) (raft.RaftClient, error) {
	c.connectionsMutex.RLock()
	conn, ok := c.connections[member]
	c.connectionsMutex.RUnlock()
	if !ok || conn.grpcConn.GetState() == connectivity.Shutdown || conn.term < term {
		if ok {
			log.Printf("Building new connection. Old term %d new term %d", conn.term, term)
			conn.grpcConn.Close()
		}
		grpcConn, err := c.buildConnection(ctx, member)
		if err != nil {
			return nil, errors.Wrapf(err, "Couldn't build new connection to %v", member)
		}
		c.connectionsMutex.Lock()
		c.connections[member] = connection{
			grpcConn: grpcConn,
			term: term,
		}
		c.connectionsMutex.Unlock()

		return raft.NewRaftClient(grpcConn), nil
	}

	return raft.NewRaftClient(conn.grpcConn), nil
}


func (c *Cluster) buildConnection(ctx context.Context, memberName string) (*grpc.ClientConn, error) {
	member, err := c.GetMember(memberName)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't get requested member")
	}
	conn, err := grpc.DialContext(ctx, fmt.Sprintf("%v:%v", member.Addr, 8001), grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't dial target node")
	}

	return conn, nil
}
