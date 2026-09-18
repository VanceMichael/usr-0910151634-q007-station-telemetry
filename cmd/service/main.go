// 基准站遥测连续性服务入口。
//
// 启动顺序：等待数据库 -> 执行嵌入式迁移 -> 监听 HTTP。
// 迁移与应用同一镜像、同一进程，compose 无需独立迁移容器。
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"

	"github.com/vancemichael/station-telemetry-service/internal/contracts"
	"github.com/vancemichael/station-telemetry-service/internal/httpapi"
	"github.com/vancemichael/station-telemetry-service/internal/store"
)

func main() {
	addr := envOr("HTTP_ADDR", ":8080")
	databaseURL := envOr("DATABASE_URL",
		"postgresql://telemetry:telemetry@localhost:5432/stations?sslmode=disable")

	policy, err := contracts.Load()
	if err != nil {
		log.Fatalf("加载契约失败: %v", err)
	}
	log.Printf("恢复契约 %s: offline_after=%ds healthy_streak=%d allowed_lateness=%ds 质量(sats>=%d pdop<=%.1f)",
		policy.Version, policy.OfflineAfterSeconds, policy.HealthyStreak, policy.AllowedLatenessSecs,
		policy.Quality.MinimumSatellites, policy.Quality.MaximumPDOP)

	db, err := openWithRetry(databaseURL)
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	defer db.Close()

	migrateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.Migrate(migrateCtx, db); err != nil {
		log.Fatalf("数据库迁移失败: %v", err)
	}
	log.Print("数据库迁移完成")

	// 一次性迁移模式：compose 可用 `service migrate` 作为独立迁移作业，
	// 执行完即退出；默认（无参数）迁移后继续提供 HTTP 服务。
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		log.Print("迁移作业完成，退出")
		return
	}

	st := store.New(db, policy)
	server := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(st, policy).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("遥测连续性服务监听 %s", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// openWithRetry 等待数据库就绪（compose 用 healthcheck 门控，这里再加一层
// 应用侧重试以应对刚启动的实例）。
func openWithRetry(url string) (*sql.DB, error) {
	var db *sql.DB
	var err error
	for attempt := 1; attempt <= 30; attempt++ {
		db, err = sql.Open("postgres", url)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = db.PingContext(ctx)
			cancel()
			if err == nil {
				return db, nil
			}
		}
		log.Printf("等待数据库就绪（第 %d 次）: %v", attempt, err)
		time.Sleep(time.Second)
	}
	return nil, err
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
