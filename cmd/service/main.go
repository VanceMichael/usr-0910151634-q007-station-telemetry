// 基准站遥测连续性服务:接收观测、维护来源水位与缺口、按合约判定恢复,
// 并提供交班视图与可续读事件流。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vancemichael/station-telemetry-service/internal/api"
	"github.com/vancemichael/station-telemetry-service/internal/contract"
	"github.com/vancemichael/station-telemetry-service/internal/store"
)

type config struct {
	ListenAddr   string
	DatabaseURL  string
	ContractPath string
	SweepEvery   time.Duration
}

func loadConfig() config {
	cfg := config{
		ListenAddr:   ":8080",
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		ContractPath: getenv("CONTRACT_PATH", "contracts/recovery-policy.json"),
		SweepEvery:   10 * time.Second,
	}
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		cfg.ListenAddr = v
	}
	if v := os.Getenv("SWEEP_INTERVAL_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.SweepEvery = time.Duration(n) * time.Second
		}
	}
	return cfg
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// connectDB 建立连接池并等待数据库就绪(容器编排下数据库可能慢于应用启动)。
func connectDB(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	for attempt := 1; ; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = pool.Ping(pingCtx)
		cancel()
		if err == nil {
			return pool, nil
		}
		if attempt >= 30 {
			pool.Close()
			return nil, err
		}
		slog.Info("等待数据库就绪", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// sweepLoop 周期性地把静默超时的来源标记为 offline。
func sweepLoop(ctx context.Context, st *store.Store, policy contract.Policy, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			swept, err := st.SweepOffline(ctx, policy.OfflineAfter())
			if err != nil {
				slog.Error("offline 清扫失败", "error", err)
			} else if swept > 0 {
				slog.Info("offline 清扫完成", "swept", swept)
			}
		}
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := loadConfig()
	if cfg.DatabaseURL == "" {
		slog.Error("缺少 DATABASE_URL")
		os.Exit(1)
	}

	policy, err := contract.Load(cfg.ContractPath)
	if err != nil {
		slog.Error("加载恢复策略失败", "error", err, "path", cfg.ContractPath)
		os.Exit(1)
	}
	slog.Info("恢复策略已加载", "version", policy.Version, "healthy_streak", policy.HealthyStreak)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := connectDB(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("连接数据库失败", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	st := store.New(pool)
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.NewServer(st, policy).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go sweepLoop(ctx, st, policy, cfg.SweepEvery)

	go func() {
		slog.Info("服务已启动", "addr", cfg.ListenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP 服务异常退出", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("正在优雅退出")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}
