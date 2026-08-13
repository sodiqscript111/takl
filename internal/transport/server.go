package transport

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"

	"takl/internal/engine/store"
	"takl/internal/model"
	"takl/internal/transport/pb"
)

type Server struct {
	pb.UnimplementedSyncServiceServer
	nodeID	string
	store	*store.Store
}

func NewServer(nodeID string, st *store.Store) *Server {
	return &Server{nodeID: nodeID, store: st}
}

func (s *Server) RegisterWith(gs *grpc.Server) {
	pb.RegisterSyncServiceServer(gs, s)
}

func (s *Server) Pull(_ context.Context, req *pb.PullRequest) (*pb.PullResponse, error) {
	resp := &pb.PullResponse{
		NodeId:		s.nodeID,
		Checksums:	make(map[string]uint64),
	}
	for _, kind := range store.Kinds {
		wm := model.HLC{}
		if h := req.Watermarks[string(kind)]; h != nil {
			wm = model.HLC{TS: h.Ts, Seq: h.Seq}
		}
		chk, err := s.store.Checksum(kind)
		if err != nil {
			return nil, err
		}
		resp.Checksums[string(kind)] = chk

		var reqChk uint64
		if req.Checksums != nil {
			reqChk = req.Checksums[string(kind)]
		}

		rows, err := s.store.ChangesSince(kind, wm)
		if err != nil {
			return nil, err
		}

		if len(rows) == 0 && reqChk != 0 && reqChk != chk {
			wm = model.HLC{}
			rows, err = s.store.ChangesSince(kind, wm)
			if err != nil {
				return nil, err
			}
		}

		for _, r := range rows {
			resp.Rows = append(resp.Rows, &pb.Row{
				Kind:		string(r.Kind),
				Key:		r.Key,
				Owner:		r.Owner,
				Hlc:		&pb.HLC{Ts: r.HLC.TS, Seq: r.HLC.Seq},
				Tombstone:	r.Tombstone,
				Payload:	r.Payload,
			})
		}
	}

	evWm := model.HLC{}
	if req.EventWatermark != nil {
		evWm = model.HLC{TS: req.EventWatermark.Ts, Seq: req.EventWatermark.Seq}
	}
	evChk, err := s.store.ChecksumEvents()
	if err != nil {
		return nil, err
	}
	resp.EventChecksum = evChk

	evs, err := s.store.EventsSince(evWm)
	if err != nil {
		return nil, err
	}
	if len(evs) == 0 && req.EventChecksum != 0 && req.EventChecksum != evChk {
		evWm = model.HLC{}
		evs, err = s.store.EventsSince(evWm)
		if err != nil {
			return nil, err
		}
	}
	for _, e := range evs {
		pl, _ := json.Marshal(e.Payload)
		resp.Events = append(resp.Events, &pb.Event{
			RunnerId:	e.RunnerID,
			Seq:		e.Seq,
			Type:		string(e.Type),
			Payload:	string(pl),
			Hlc:		&pb.HLC{Ts: e.HLC.TS, Seq: e.HLC.Seq},
		})
	}

	return resp, nil
}
