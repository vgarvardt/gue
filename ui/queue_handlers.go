package asynqmon

import (
	"encoding/json"
	"net/http"
	"time"

	database2 "github.com/2tvenom/guex/database"
	"github.com/gorilla/mux"
	"golang.org/x/exp/maps"
)

// ****************************************************************************
// This file defines:
//   - http.Handler(s) for queue related endpoints
// ****************************************************************************

func newListQueuesHandlerFunc(inspector *database2.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			totalInfo []database2.GetTotalInfoRow
			err       error
		)
		if totalInfo, err = inspector.GetTotalInfo(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var totalInfoErr []database2.GetTotalInfoWithErrorsRow
		if totalInfoErr, err = inspector.GetTotalInfoWithErrors(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var snapshots = make(map[string]*queueStateSnapshot)

		for _, i := range totalInfo {
			if _, ok := snapshots[i.Queue]; !ok {
				snapshots[i.Queue] = &queueStateSnapshot{
					Queue:     i.Queue,
					Timestamp: time.Now(),
				}
			}

			switch i.Status {
			case database2.JobStatusPending:
				snapshots[i.Queue].Pending = int(i.Cnt)
				//snapshots[i.Queue].Scheduled = int(i.Cnt)
			case database2.JobStatusProcessing:
				snapshots[i.Queue].Active = int(i.Cnt)
			case database2.JobStatusFinished:
				snapshots[i.Queue].Completed = int(i.Cnt)
				snapshots[i.Queue].Processed += int(i.Cnt)
				snapshots[i.Queue].Succeeded = int(i.Cnt)
			case database2.JobStatusFailed:
				snapshots[i.Queue].Failed = int(i.Cnt)
				snapshots[i.Queue].Retry = int(i.Cnt)
				snapshots[i.Queue].Processed += int(i.Cnt)
			}
			snapshots[i.Queue].Size += int(i.Cnt)
		}

		for _, i := range totalInfoErr {
			if _, ok := snapshots[i.Queue]; !ok {
				snapshots[i.Queue] = &queueStateSnapshot{
					Queue:     i.Queue,
					Timestamp: time.Now(),
				}
			}
			snapshots[i.Queue].Retry += int(i.Cnt)
			snapshots[i.Queue].Size += int(i.Cnt)
		}

		payload := map[string]interface{}{"queues": maps.Values(snapshots)}
		json.NewEncoder(w).Encode(payload)
	}
}

func newGetQueueHandlerFunc(inspector *database2.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		qname := vars["qname"]

		var (
			err   error
			total []database2.GetTotalInfoByNameRow
		)

		if total, err = inspector.GetTotalInfoByName(r.Context(), qname); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		payload := make(map[string]interface{})
		var snap = &queueStateSnapshot{
			Queue: qname,
		}

		for _, i := range total {
			switch i.Status {
			case database2.JobStatusPending:
				snap.Pending = int(i.Cnt)
				snap.Scheduled = int(i.Cnt)
			case database2.JobStatusProcessing:
				snap.Active = int(i.Cnt)
			case database2.JobStatusFinished:
				snap.Completed = int(i.Cnt)
				snap.Processed += int(i.Cnt)
				snap.Succeeded = int(i.Cnt)
			case database2.JobStatusFailed:
				snap.Failed = int(i.Cnt)
				snap.Processed += int(i.Cnt)
			}
			snap.Size += int(i.Cnt)
		}

		payload["current"] = snap

		var history []database2.GetTotalInfoByNameHistoryRow
		if history, err = inspector.GetTotalInfoByNameHistory(r.Context(), qname); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var h = make(map[string]*dailyStats)
		for _, s := range history {
			var d = s.Date.Format(time.DateOnly)
			if _, ok := h[d]; !ok {
				h[d] = &dailyStats{
					Queue: s.Queue,
					Date:  d,
				}
			}

			switch s.Status {
			case database2.JobStatusFinished:
				h[d].Processed += int(s.Cnt)
			case database2.JobStatusFailed:
				h[d].Failed = int(s.Cnt)
			}
		}
		payload["history"] = maps.Values(h)
		json.NewEncoder(w).Encode(payload)
	}
}

func newListQueueStatsHandlerFunc(inspector *database2.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		type listQueueStatsResponse struct {
			Stats map[string][]*dailyStats `json:"stats"`
		}

		var (
			history []database2.GetTotalInfoHistoryRow
			err     error
		)
		if history, err = inspector.GetTotalInfoHistory(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var h = make(map[string]map[string]*dailyStats)

		resp := listQueueStatsResponse{
			Stats: map[string][]*dailyStats{},
		}

		for _, s := range history {
			var d = s.Date.Format(time.DateOnly)
			if _, ok := h[s.Queue]; !ok {
				h[s.Queue] = map[string]*dailyStats{}
			}

			if _, ok := h[s.Queue][d]; !ok {
				h[s.Queue][d] = &dailyStats{}
			}

			switch s.Status {
			case database2.JobStatusFinished:
				h[s.Queue][d].Processed += int(s.Cnt)
			case database2.JobStatusFailed:
				h[s.Queue][d].Failed = int(s.Cnt)
			}
		}

		for qName, v := range h {
			resp.Stats[qName] = maps.Values(v)
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

//
//func newDeleteQueueHandlerFunc(inspector *database.Queries) http.HandlerFunc {
//	return func(w http.ResponseWriter, r *http.Request) {
//		vars := mux.Vars(r)
//		qname := vars["qname"]
//		if err := inspector.DeleteQueue(qname, false); err != nil {
//			if errors.Is(err, asynq.ErrQueueNotFound) {
//				http.Error(w, err.Error(), http.StatusNotFound)
//				return
//			}
//			if errors.Is(err, asynq.ErrQueueNotEmpty) {
//				http.Error(w, err.Error(), http.StatusBadRequest)
//				return
//			}
//			http.Error(w, err.Error(), http.StatusInternalServerError)
//			return
//		}
//		w.WriteHeader(http.StatusNoContent)
//	}
//}
//
//func newPauseQueueHandlerFunc(inspector *database.Queries) http.HandlerFunc {
//	return func(w http.ResponseWriter, r *http.Request) {
//		vars := mux.Vars(r)
//		qname := vars["qname"]
//		if err := inspector.PauseQueue(qname); err != nil {
//			http.Error(w, err.Error(), http.StatusInternalServerError)
//			return
//		}
//		w.WriteHeader(http.StatusNoContent)
//	}
//}
//
//func newResumeQueueHandlerFunc(inspector *database.Queries) http.HandlerFunc {
//	return func(w http.ResponseWriter, r *http.Request) {
//		vars := mux.Vars(r)
//		qname := vars["qname"]
//		if err := inspector.UnpauseQueue(qname); err != nil {
//			http.Error(w, err.Error(), http.StatusInternalServerError)
//			return
//		}
//		w.WriteHeader(http.StatusNoContent)
//	}
//}
