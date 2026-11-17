package transport

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"takl/internal/engine/store"
	"takl/internal/metrics"
	"takl/internal/transport/pb"
)

type Server struct {
	pb.UnimplementedSyncServiceServer
	nodeID string
	store  *store.Store
}

func NewServer(nodeID string, st *store.Store) *Server {
	return &Server{nodeID: nodeID, store: st}
}

func (s *Server) RegisterWith(gs *grpc.Server) {
	pb.RegisterSyncServiceServer(gs, s)
}

func (s *Server) Pull(_ context.Context, req *pb.PullRequest) (*pb.PullResponse, error) {
	if req.Forgotten && req.PeerStoreId != "" {
		id, err := s.store.StoreID()
		if err != nil {
			return nil, err
		}
		if req.PeerStoreId == id {
			if err := Wipe(s.store, "peer "+req.NodeId+" has forgotten this store"); err != nil {
				return nil, err
			}
		}
	}

	res, err := s.store.Serve(store.ServeRequest{
		StoreID:     req.StoreId,
		NodeID:      req.NodeId,
		PeerStoreID: req.PeerStoreId,
		Since:       req.SinceVersion,
		NowMs:       time.Now().UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	resp := &pb.PullResponse{
		NodeId:    s.nodeID,
		StoreId:   res.StoreID,
		Version:   res.Version,
		Forgotten: res.Forgotten,
	}
	for _, r := range res.Rows {
		resp.Rows = append(resp.Rows, &pb.Row{
			Kind:      string(r.Kind),
			Key:       r.Key,
			Owner:     r.Owner,
			Hlc:       &pb.HLC{Ts: r.HLC.TS, Seq: r.HLC.Seq},
			Tombstone: r.Tombstone,
			Payload:   r.Payload,
		})
	}
	for _, e := range res.Events {
		pl, _ := json.Marshal(e.Payload)
		resp.Events = append(resp.Events, &pb.Event{
			RunnerId: e.RunnerID,
			Seq:      e.Seq,
			Type:     string(e.Type),
			Payload:  string(pl),
			Hlc:      &pb.HLC{Ts: e.HLC.TS, Seq: e.HLC.Seq},
			Origin:   e.Origin,
		})
	}
	return resp, nil
}

// Wipe wipes st after a peer reported it has forgotten this store, and logs it.
func Wipe(st *store.Store, reason string) error {
	id, kept, err := st.Wipe()
	if err != nil {
		return err
	}
	metrics.SyncWipes.Inc()
	slog.Warn("store wiped: a peer forgot this node, resyncing from scratch",
		"reason", reason, "new_store_id", id, "own_unsent_rows_kept", kept)
	return nil
}
