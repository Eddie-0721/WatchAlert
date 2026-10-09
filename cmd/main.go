package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"watchAlert/alert"
	"watchAlert/config"
	"watchAlert/internal/cache"
	"watchAlert/internal/ctx"
	"watchAlert/internal/middleware"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/internal/routers"
	v1 "watchAlert/internal/routers/v1"
	"watchAlert/internal/services"
	"watchAlert/pkg/ai"

	"github.com/gin-gonic/gin"
	"github.com/zeromicro/go-zero/core/logc"
	"golang.org/x/sync/errgroup"
)

var Version string

func main() {
	// 初始化配置
	config.InitConfig(Version)
	logc.Info(context.Background(), "服务启动")

	initBasic()

	mode := config.Application.Server.Mode
	if mode == "" {
		mode = gin.DebugMode
	}
	gin.SetMode(mode)
	ginEngine := gin.New()
	ginEngine.Use(
		// 启用CORS中间件
		middleware.Cors(),
		// 自定义请求日志格式
		middleware.GinZapLogger(),
		gin.Recovery(),
		middleware.LoggingMiddleware(),
	)

	initRouter(ginEngine)

	go func() {
		panic(http.ListenAndServe("localhost:9999", nil))
	}()

	err := ginEngine.Run(":" + config.Application.Server.Port)
	if err != nil {
		panic(fmt.Sprintf("服务启动失败: %s", err.Error()))
	}
}

func initRouter(engine *gin.Engine) {
	routers.HealthCheck(engine)
	v1.Router(engine)
}

func initBasic() {
	// 初始化数据库和缓存
	dbRepo := repo.NewRepoEntry()
	rCache := cache.NewEntryCache()

	// 创建上下文
	ctx := ctx.NewContext(context.Background(), dbRepo, rCache)

	// 初始化服务
	services.NewServices(ctx)

	// 导入数据源 Client 到存储池
	importClientPools(ctx)

	// Install clients before evaluators can acquire them.
	alert.Initialize(ctx)

	// 加载静默规则
	go pushMuteRuleToRedis()

	r, err := ctx.DB.Setting().Get()
	if err != nil {
		logc.Error(ctx.Ctx, fmt.Sprintf("加载系统设置失败: %s", err.Error()))
		return
	}

	if r.AuthType != nil && *r.AuthType == models.SettingLdapAuth {
		const mark = "SyncLdapUserJob"
		c, cancel := context.WithCancel(context.Background())
		ctx.ContextMap[mark] = cancel
		go services.LdapService.SyncUsersCronjob(c, r.LdapConfig)
	}

	// 启动年度值班表生成定时任务
	const dutyScheduleMark = "GenerateDutyScheduleJob"
	dutyScheduleCtx, dutyScheduleCancelFunc := context.WithCancel(context.Background())
	ctx.ContextMap[dutyScheduleMark] = dutyScheduleCancelFunc
	go services.DutyCalendarService.GenerateNextYearScheduleCronjob(dutyScheduleCtx)

	if r.AiConfig.GetEnable() {
		client, err := ai.NewAiClient(&r.AiConfig)
		if err != nil {
			logc.Error(ctx.Ctx, fmt.Sprintf("创建 Ai 客户端失败: %s", err.Error()))
			return
		}
		ctx.Redis.ProviderPools().SetClient("AiClient", client)
	}
}

func importClientPools(ctx *ctx.Context) {
	list, err := ctx.DB.Datasource().List("", "", "", "")
	if err != nil {
		logc.Error(ctx.Ctx, err.Error())
		return
	}

	if err := initializeDatasourceClients(list, services.DatasourceService.WithAddClientToProviderPools); err != nil {
		logc.Error(ctx.Ctx, "Some datasource clients could not be initialized; affected evaluations will report client unavailable")
	}
}

func initializeDatasourceClients(list []models.AlertDataSource, create func(models.AlertDataSource) error) error {
	g := new(errgroup.Group)
	g.SetLimit(8)
	for _, datasource := range list {
		ds := datasource
		if !ds.GetEnabled() {
			continue
		}
		g.Go(func() error { return create(ds) })
	}
	return g.Wait()
}

func pushMuteRuleToRedis() {
	loaded, err := services.LoadSilenceCache(ctx.DB.Silence(), ctx.Redis.Silence())
	if err != nil {
		logc.Errorf(ctx.Ctx, "静默加载未完成，已同步 %d 条: %v", loaded, err)
		return
	}
	logc.Infof(ctx.Ctx, "静默规则加载完成，共 %d 条", loaded)
}
