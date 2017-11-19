package grpccache

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type Cache struct {
	connections map[address]*grpc.ClientConn
	requests    chan *connectionRequest
}

type connectionRequest struct {
	ctx          context.Context
	address      address
	responseChan chan<- *finishedConnection
}

type address string

type finishedConnection struct {
	address    address
	connection *grpc.ClientConn
	err        error
}

func NewCache(ctx context.Context) *Cache {
	cache := &Cache{
		connections: make(map[address]*grpc.ClientConn),
		requests:    make(chan *connectionRequest),
	}
	go cache.loop(ctx)

	return cache
}

func (cache *Cache) GetConnection(ctx context.Context, address address) (*grpc.ClientConn, error) {
	responseChan := make(chan *finishedConnection)

	select {
	case cache.requests <- &connectionRequest{
		ctx:          ctx,
		address:      address,
		responseChan: responseChan,
	}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case res := <-responseChan:
		return res.connection, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (cache *Cache) loop(ctx context.Context) {
	waiting := make(map[address][]*connectionRequest)
	finished := make(chan *finishedConnection)

	for {
		select {
		case req := <-cache.requests:
			// Just send the connection over if it exists
			if conn, ok := cache.connections[req.address]; ok {
				// This sends the connection over if the client is waiting on the other side. Aborts otherwise.
				// This also aborts if the cache context gets cancelled.
				select {
				case req.responseChan <- &finishedConnection{
					address:    req.address,
					connection: conn,
				}:
				case <-req.ctx.Done():
				case <-ctx.Done():
				}
				close(req.responseChan)
				continue
			}

			// If there are already existing clients waiting for this connection, then a worker thread
			// creating it is in progress already, just add this client as another one waiting for it.
			if alreadyWaiting, ok := waiting[req.address]; ok {
				waiting[req.address] = append(alreadyWaiting, req)
				continue
			}

			// If there is no one already waiting, start a new worker to create this connection.
			waiting[req.address] = []*connectionRequest{req}
			go buildConnection(ctx, req.address, finished)

		case conn := <-finished:
			if conn.err == nil {
				cache.connections[conn.address] = conn.connection
			}

			for _, client := range waiting[conn.address] {
				// Send it over if the client is still there. Abort otherwise.
				// This also aborts if the cache context gets cancelled.
				select {
				case client.responseChan <- conn:
				case <-client.ctx.Done():
				case <-ctx.Done():
				}
				close(client.responseChan)
			}
			delete(waiting, conn.address)

		case <-ctx.Done():
			return
		}
	}
}

func buildConnection(ctx context.Context, address address, finished chan<- *finishedConnection) {
	conn, err := grpc.DialContext(ctx, string(address), grpc.WithInsecure())
	if err != nil {
		finished <- &finishedConnection{
			address: address,
			err:     errors.Wrapf(err, "Couldn't dial grpc. Address: %s", address),
		}
		return
	}

	finishedConn := finishedConnection{
		address:    address,
		connection: conn,
	}

	// In case the cache gets closed, by cancellation of the context, we abort sending the connection over.
	select {
	case finished <- &finishedConn:
	case <-ctx.Done():
		conn.Close()
		return
	}
}
