package medium

import (
	"context"
	"errors"
	"fmt"
	"net/smtp"
	"watchAlert/internal/ctx"

	"github.com/jordan-wright/email"
)

// EmailSender 邮件发送策略
type EmailSender struct {
	ServerAddr string
	Port       int
	Email      *email.Email
	Auth       smtp.Auth
}

func NewEmailSender() (SendInter, error) {
	setting, err := ctx.DB.Setting().Get()
	if err != nil {
		return nil, errors.New("获取 系统配置/邮箱配置 失败: " + err.Error())
	}

	e := email.NewEmail()
	auth := smtp.PlainAuth("", setting.CommunicationConfig.Email.Email, setting.CommunicationConfig.Email.Token, setting.CommunicationConfig.Email.ServerAddress)
	e.From = fmt.Sprintf("WatchAlert<%s>", setting.CommunicationConfig.Email.Email)

	return &EmailSender{
		ServerAddr: setting.CommunicationConfig.Email.ServerAddress,
		Port:       setting.CommunicationConfig.Email.Port,
		Email:      e,
		Auth:       auth,
	}, nil
}

func (e *EmailSender) Send(params SendParams) error {
	if params.IsRecovered {
		params.Email.Subject = params.Email.Subject + "「已恢复」"
	} else {
		params.Email.Subject = params.Email.Subject + "「报警中」"
	}
	err := e.postContext(params.Context(), params.Email.To, params.Email.CC, params.Email.Subject, []byte(params.Content))
	if err != nil {
		return fmt.Errorf("send email: %w", err)
	}

	return nil
}

func (e *EmailSender) Test(params SendParams) error {
	return e.postContext(params.Context(), params.Email.To, params.Email.CC, "WatchAlert 消息测试", []byte(RobotTestContent))
}

func (e *EmailSender) post(to, cc []string, subject string, msg []byte) error {
	return e.postContext(context.Background(), to, cc, subject, msg)
}

func (e *EmailSender) postContext(requestCtx context.Context, to, cc []string, subject string, msg []byte) error {
	// Keep per-send fields local, including when a sender is reused.
	message := *e.Email
	message.To = to
	message.Cc = cc
	message.HTML = msg
	message.Subject = subject
	return sendSMTP(requestCtx, e.ServerAddr, e.Port, e.Auth, &message)
}
