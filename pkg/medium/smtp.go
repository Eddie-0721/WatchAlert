package medium

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/mail"
	"net/smtp"
	"strconv"
	"time"

	"github.com/jordan-wright/email"
)

const smtpSendTimeout = 30 * time.Second

// The legacy email helper has no dial, greeting, write or reply deadline.
// Keep its MIME builder and SMTP semantics, but bound the complete exchange.
func sendSMTP(parent context.Context, host string, port int, auth smtp.Auth, message *email.Email) error {
	requestCtx, cancel := context.WithTimeout(parent, smtpSendTimeout)
	defer cancel()
	if err := requestCtx.Err(); err != nil {
		return err
	}
	recipients := make([]string, 0, len(message.To)+len(message.Cc)+len(message.Bcc))
	for _, group := range [][]string{message.To, message.Cc, message.Bcc} {
		for _, value := range group {
			address, err := mail.ParseAddress(value)
			if err != nil {
				return err
			}
			recipients = append(recipients, address.Address)
		}
	}
	if message.From == "" || len(recipients) == 0 {
		return errors.New("email requires a sender and at least one recipient")
	}
	envelopeFrom := message.Sender
	if envelopeFrom == "" {
		envelopeFrom = message.From
	}
	sender, err := mail.ParseAddress(envelopeFrom)
	if err != nil {
		return err
	}
	raw, err := message.Bytes()
	if err != nil {
		return err
	}
	tlsConfig := &tls.Config{ServerName: host}
	err = smtpExchange(requestCtx, net.JoinHostPort(host, strconv.Itoa(port)), tlsConfig, port == 465, auth, sender.Address, recipients, raw)
	if requestCtx.Err() != nil && err != nil {
		return requestCtx.Err()
	}
	return err
}

func smtpExchange(requestCtx context.Context, addr string, tlsConfig *tls.Config, implicitTLS bool, auth smtp.Auth, from string, recipients []string, raw []byte) error {
	conn, err := (&net.Dialer{}).DialContext(requestCtx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadline, ok := requestCtx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return err
		}
	}
	// Closing the real socket interrupts all SMTP phases, not just the caller.
	stop := context.AfterFunc(requestCtx, func() { _ = conn.Close() })
	defer stop()
	var smtpConn net.Conn = conn
	if implicitTLS {
		tlsConn := tls.Client(conn, tlsConfig)
		if err := tlsConn.HandshakeContext(requestCtx); err != nil {
			return err
		}
		smtpConn = tlsConn
	}
	client, err := smtp.NewClient(smtpConn, tlsConfig.ServerName)
	if err != nil {
		return err
	}
	defer client.Close()
	if !implicitTLS {
		if ok, _ := client.Extension("STARTTLS"); ok {
			if err := client.StartTLS(tlsConfig); err != nil {
				return err
			}
		}
	}
	if auth != nil {
		if ok, _ := client.Extension("AUTH"); !ok {
			return errors.New("SMTP server does not support authentication")
		}
		if err := client.Auth(auth); err != nil {
			return err
		}
	}
	if err := client.Mail(from); err != nil {
		return err
	}
	for _, recipient := range recipients {
		if err := client.Rcpt(recipient); err != nil {
			return err
		}
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	if _, err := writer.Write(raw); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return client.Quit()
}
