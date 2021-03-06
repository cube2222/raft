package cluster

import (
	"context"
	"fmt"
	"log"

	grpccache "github.com/cube2222/grpc-connection-cache"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type Cluster struct {
	cluster *serf.Serf

	connectionCache *grpccache.ConnectionCache
}

func NewCluster(ctx context.Context, localNodeName string, clusterAddresses []string) (*Cluster, error) {
	cluster, err := setupCluster(
		localNodeName,
		clusterAddresses,
	)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't setup cluster")
	}

	return &Cluster{
		cluster:         cluster,
		connectionCache: grpccache.NewConnectionCache(ctx),
	}, nil
}

func setupCluster(nodeName string, clusterAddresses []string) (*serf.Serf, error) {
	conf := serf.DefaultConfig()
	conf.Init()

	conf.MemberlistConfig.Name = nodeName

	cluster, err := serf.Create(conf)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't create cluster")
	}

	_, err = cluster.Join(clusterAddresses, true)
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

func (c *Cluster) GetgRPCConnection(ctx context.Context, member string, port int) (*grpc.ClientConn, error) {
	memberInfo, err := c.GetMember(member)
	if err != nil {
		return nil, errors.Wrap(err, "Inexistant member")
	}

	conn, err := c.connectionCache.GetConnection(ctx, fmt.Sprintf("%v:%v", memberInfo.Addr, port))
	if err != nil {
		return nil, errors.Wrapf(err, "Couldn't get connection to member: %v", member)
	}

	return conn, nil
}
