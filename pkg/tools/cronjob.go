package tools

import (
	"context"
	"github.com/robfig/cron/v3"
	"github.com/zeromicro/go-zero/core/logc"
)

func NewCronjob(spec string, cmd func()) {
	c := cron.New()
	_, err := c.AddFunc(spec, cmd)
	if err != nil {
		logc.Error(context.Background(), err)
		return
	}
	c.Start()
	defer c.Stop()

	select {}
}
