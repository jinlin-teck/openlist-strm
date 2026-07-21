// Package api 提供配置管理与任务触发的 HTTP 接口，并内嵌 WebUI。
package api

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"openlist-strm/internal/app"
	"openlist-strm/internal/config"
)

//go:embed web
var webFS embed.FS

// Server 是 HTTP 服务。
type Server struct {
	app *app.App
	mux *http.ServeMux
}

func New(a *app.App) *Server {
	s := &Server{app: a, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/config", s.getConfig)
	s.mux.HandleFunc("PUT /api/config", s.putConfig)
	s.mux.HandleFunc("GET /api/tasks", s.listTasks)
	s.mux.HandleFunc("POST /api/tasks/{id}/run", s.runTask)
	s.mux.HandleFunc("POST /api/alist/test", s.testAlist)

	static, err := fs.Sub(webFS, "web")
	if err != nil {
		panic(err)
	}
	s.mux.Handle("GET /", http.FileServerFS(static))
}

func (s *Server) Handler() http.Handler { return s.mux }

// --- handlers ---

func (s *Server) getConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.app.Config())
}

func (s *Server) putConfig(w http.ResponseWriter, r *http.Request) {
	var cfg config.Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON: "+err.Error())
		return
	}
	if err := s.app.UpdateConfig(&cfg); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// TaskView 是任务配置 + 运行状态 + 下次触发时间的组合视图。
type TaskView struct {
	config.TaskConfig
	Running   bool       `json:"running"`
	LastStart *time.Time `json:"last_start,omitempty"`
	LastEnd   *time.Time `json:"last_end,omitempty"`
	LastError string     `json:"last_error,omitempty"`
	Stats     any        `json:"stats,omitempty"`
	NextRun   *time.Time `json:"next_run,omitempty"`
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	cfg := s.app.Config()
	statuses := s.app.Statuses()
	nextRuns := s.app.NextRuns()
	views := make([]TaskView, 0, len(cfg.Tasks))
	for _, t := range cfg.Tasks {
		v := TaskView{TaskConfig: t}
		if st, ok := statuses[t.ID]; ok {
			v.Running = st.Running
			v.LastStart = st.LastStart
			v.LastEnd = st.LastEnd
			v.LastError = st.LastError
			if st.Stats != nil {
				v.Stats = st.Stats
			}
		}
		if n, ok := nextRuns[t.ID]; ok {
			v.NextRun = &n
		}
		views = append(views, v)
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) runTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// 异步执行，立即返回；状态通过 GET /api/tasks 轮询。
	go func() {
		if _, err := s.app.RunTask(context.Background(), id); err != nil {
			slog.Warn("手动触发任务结束", "task", id, "err", err)
		}
	}()
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "started"})
}

type testReq struct {
	BaseURL string `json:"base_url"`
	Token   string `json:"token"`
}

func (s *Server) testAlist(w http.ResponseWriter, r *http.Request) {
	var req testReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	if err := app.TestAlist(ctx, req.BaseURL, req.Token); err != nil {
		writeError(w, http.StatusOK, "连接失败: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
