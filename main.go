// openlist-strm：从 OpenList/Alist 生成 .strm 文件的轻量服务，带 WebUI。
package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"openlist-strm/internal/api"
	"openlist-strm/internal/app"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "配置文件路径")
	debug := flag.Bool("debug", false, "输出调试日志")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	if _, err := os.Stat(*cfgPath); errors.Is(err, os.ErrNotExist) {
		abs, _ := filepath.Abs(*cfgPath)
		fmt.Fprintf(os.Stderr, "配置文件不存在: %s\n请参考 config.example.yaml 创建后重试。\n", abs)
		os.Exit(1)
	}

	a, err := app.New(*cfgPath, log)
	if err != nil {
		log.Error("启动失败", "err", err)
		os.Exit(1)
	}
	defer a.Close()

	srv := &http.Server{
		Addr:              a.Config().Server.Listen,
		Handler:           api.New(a).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info("WebUI 已启动", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP 服务异常退出", "err", err)
			os.Exit(1)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("正在退出...")
}
