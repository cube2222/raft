package grpccache

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type Cache struct {
	connsMutex  sync.RWMutex
	connections map[address]*grpc.ClientConn
	requests    <-chan *connectionRequest
}

type connectionRequest struct {
	address      address
	responseChan chan<- *finishedConnection
}

type address string

type finishedConnection struct {
	address    address
	connection *grpc.ClientConn
	err        error
}

func (cache *Cache) GetConnection(ctx context.Context, address address) (*grpc.ClientConn, error) {
	cache.connsMutex.RLock()
	conn, ok := cache.connections[address]
	cache.connsMutex.RUnlock()
	if ok {
		return conn, nil
	}

	responseChan := make(chan *finishedConnection)

	cache.requests <- &connectionRequest{
		address:      address,
		responseChan: responseChan,
	}

	select {
	case res := <-responseChan:
		if res == nil {
			// This means we were too late to listen, had been ignored and received nil.
			// But the connection should now be in the connection cache, so we can just take it from there.
			cache.connsMutex.RLock()
			conn, ok := cache.connections[address]
			cache.connsMutex.RUnlock()
			if !ok {
				return nil, errors.New("Got nil connection, this really shouldn't happen. Concurrency Bug.")
			}

			return conn, nil
		}
		return res.connection, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (cache *Cache) loop(ctx context.Context) {
	waiting := make(map[address][]chan<- *finishedConnection)
	finished := make(chan *finishedConnection)
	for {
		select {
		case req := <-cache.requests:
			// Just send the connection over if it exists
			cache.connsMutex.RLock()
			conn, ok := cache.connections[req.address]
			cache.connsMutex.RUnlock()
			if ok {
				// This sends the connection over if the client is waiting on the other side. Aborts otherwise.
				select {
				case req.responseChan <- &finishedConnection{
					address:    req.address,
					connection: conn,
				}:
				default:
				}
				close(req.responseChan)
				continue
			}

			// If there are already existing clients waiting for this connection, then a worker thread
			// creating it is in progress already, just add this client as another one waiting for it.
			if alreadyWaiting, ok := waiting[req.address]; ok {
				waiting[req.address] = append(alreadyWaiting, req.responseChan)
				continue
			}

			// If there is no one already waiting, start a new worker to create this connection.
			waiting[req.address] = [](chan<- *finishedConnection){req.responseChan}
			go buildConnection(ctx, req.address, finished)
		case conn := <-finished:
			if conn.err == nil {
				cache.connsMutex.Lock()
				cache.connections[conn.address] = conn.connection
				cache.connsMutex.Unlock()
			}

			for _, client := range waiting[conn.address] {
				// Send it over if the client is still there, otherwise ignore him and close the channel.
				select {
				case client <- conn:
				default:
				}
				close(client)
			}
		}
	}
}

func buildConnection(ctx context.Context, address address, finished chan<- *finishedConnection) {
	conn, err := grpc.DialContext(ctx, string(address), grpc.WithInsecure())
	if err != nil {
		finished <- &finishedConnection{
			address: address,
			err:     errors.Wrapf(err, "Couldn't dial grpc. Address: %v", address),
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
