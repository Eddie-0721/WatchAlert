package medium

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"net/mail"
	"net/smtp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jordan-wright/email"
)

// Only loopback SMTP fixtures are used. Never contact configured mail servers.
func smtpListener(t *testing.T, handler func(net.Conn)) (string, int, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("SMTP fixture did not stop")
		}
	})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		handler(conn)
	}()
	host, portString, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portString)
	return host, port, done
}

func testEmail() *email.Email {
	e := email.NewEmail()
	e.From = "WatchAlert <sender@example.test>"
	e.To = []string{"Receiver <to@example.test>"}
	e.Cc = []string{"copy@example.test"}
	e.Bcc = []string{"hidden@example.test"}
	e.Subject = "通知测试"
	e.HTML = []byte("<p>test alert</p>")
	return e
}

func TestSMTPDeliversMIMEAndAllEnvelopeRecipients(t *testing.T) {
	commands := make(chan []string, 1)
	message := make(chan string, 1)
	host, port, _ := smtpListener(t, func(conn net.Conn) {
		_, _ = io.WriteString(conn, "220 local SMTP\r\n")
		reader := bufio.NewReader(conn)
		var seen []string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSpace(line)
			seen = append(seen, line)
			switch {
			case strings.HasPrefix(line, "EHLO"):
				_, _ = io.WriteString(conn, "250-local\r\n250 8BITMIME\r\n")
			case line == "DATA":
				_, _ = io.WriteString(conn, "354 send message\r\n")
				var body strings.Builder
				for {
					line, err = reader.ReadString('\n')
					if err != nil {
						return
					}
					if line == ".\r\n" {
						break
					}
					body.WriteString(line)
				}
				message <- body.String()
				_, _ = io.WriteString(conn, "250 queued\r\n")
			case line == "QUIT":
				_, _ = io.WriteString(conn, "221 bye\r\n")
				commands <- seen
				return
			default:
				_, _ = io.WriteString(conn, "250 ok\r\n")
			}
		}
	})
	if err := sendSMTP(context.Background(), host, port, nil, testEmail()); err != nil {
		t.Fatal(err)
	}
	seen := strings.Join(<-commands, "\n")
	for _, want := range []string{"MAIL FROM:<sender@example.test>", "RCPT TO:<to@example.test>", "RCPT TO:<copy@example.test>", "RCPT TO:<hidden@example.test>"} {
		if !strings.Contains(seen, want) {
			t.Fatalf("missing %s in %s", want, seen)
		}
	}
	body := <-message
	parsed, err := mail.ReadMessage(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Header.Get("Bcc") != "" {
		t.Fatal("Bcc leaked into MIME headers")
	}
	if parsed.Header.Get("Subject") == "" || !strings.Contains(body, "test alert") {
		t.Fatal("MIME body lost")
	}
}

func TestSMTPCancellationClosesStalledGreetingAndReply(t *testing.T) {
	for _, phase := range []string{"greeting", "reply"} {
		t.Run(phase, func(t *testing.T) {
			started := make(chan struct{})
			host, port, done := smtpListener(t, func(conn net.Conn) {
				if phase == "reply" {
					_, _ = io.WriteString(conn, "220 local SMTP\r\n")
					if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
						return
					}
				}
				close(started)
				_, _ = io.Copy(io.Discard, conn)
			})
			requestCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- sendSMTP(requestCtx, host, port, nil, testEmail()) }()
			select {
			case <-started:
			case <-requestCtx.Done():
				t.Fatal("SMTP did not start")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("got %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("SMTP cancellation did not interrupt IO")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("SMTP socket not closed")
			}
		})
	}
}

func TestSMTPDeadlineWithoutExplicitCancellation(t *testing.T) {
	host, port, _ := smtpListener(t, func(conn net.Conn) { _, _ = io.Copy(io.Discard, conn) })
	requestCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := sendSMTP(requestCtx, host, port, nil, testEmail())
	// Socket deadlines can fire just before the context timer.
	var networkError net.Error
	if !errors.Is(err, context.DeadlineExceeded) && !(errors.As(err, &networkError) && networkError.Timeout()) {
		t.Fatalf("wanted timeout, got %v", err)
	}
}

func TestSMTPCancellationInterruptsTLSHandshake(t *testing.T) {
	for _, implicit := range []bool{false, true} {
		t.Run(fmt.Sprint(implicit), func(t *testing.T) {
			started := make(chan struct{})
			host, port, done := smtpListener(t, func(conn net.Conn) {
				if !implicit {
					reader := bufio.NewReader(conn)
					_, _ = io.WriteString(conn, "220 local SMTP\r\n")
					if _, err := reader.ReadString('\n'); err != nil {
						return
					}
					_, _ = io.WriteString(conn, "250-local\r\n250 STARTTLS\r\n")
					if _, err := reader.ReadString('\n'); err != nil {
						return
					}
					_, _ = io.WriteString(conn, "220 begin TLS\r\n")
				}
				close(started)
				_, _ = io.Copy(io.Discard, conn)
			})
			requestCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				result <- smtpExchange(requestCtx, net.JoinHostPort(host, strconv.Itoa(port)), &tls.Config{ServerName: host}, implicit, nil, "sender@example.test", []string{"to@example.test"}, []byte("test"))
			}()
			select {
			case <-started:
			case <-requestCtx.Done():
				t.Fatal("TLS did not start")
			}
			cancel()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("canceled TLS reported success")
				}
			case <-time.After(time.Second):
				t.Fatal("TLS cancellation blocked")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("TLS socket not closed")
			}
		})
	}
}

func TestSMTPRejectsUntrustedTLSCertificate(t *testing.T) {
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	requestCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := smtpExchange(requestCtx, strings.TrimPrefix(server.URL, "https://"), &tls.Config{ServerName: "127.0.0.1"}, true, nil, "sender@example.test", []string{"to@example.test"}, []byte("test"))
	if err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("untrusted TLS accepted: %v", err)
	}
}

func TestSMTPCanceledBeforeDialAndInvalidEnvelope(t *testing.T) {
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sendSMTP(requestCtx, "invalid", 465, nil, testEmail()); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	e := testEmail()
	e.To = []string{"not an address"}
	if err := sendSMTP(context.Background(), "invalid", 465, nil, e); err == nil {
		t.Fatal("invalid envelope accepted")
	}
}

func TestSMTPConfiguredAuthCannotBeSilentlySkipped(t *testing.T) {
	host, port, _ := smtpListener(t, func(conn net.Conn) {
		_, _ = io.WriteString(conn, "220 local SMTP\r\n")
		reader := bufio.NewReader(conn)
		_, _ = reader.ReadString('\n')
		_, _ = io.WriteString(conn, "250 local\r\n")
		_, _ = io.Copy(io.Discard, conn)
	})
	err := sendSMTP(context.Background(), host, port, smtp.PlainAuth("", "user", "secret", host), testEmail())
	if err == nil || !strings.Contains(err.Error(), "authentication") {
		t.Fatalf("got %v", err)
	}
}

func TestSMTPAuthenticatedTLSDelivery(t *testing.T) {
	certificateServer := httptest.NewTLSServer(nil)
	certificate := certificateServer.TLS.Certificates[0]
	pool := x509.NewCertPool()
	pool.AddCert(certificateServer.Certificate())
	certificateServer.Close()
	for _, implicit := range []bool{false, true} {
		t.Run(fmt.Sprint(implicit), func(t *testing.T) {
			authed := make(chan struct{}, 1)
			host, port, _ := smtpListener(t, func(raw net.Conn) {
				conn := raw
				encrypted := implicit
				if implicit {
					conn = tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{certificate}})
				}
				_, _ = io.WriteString(conn, "220 local SMTP\r\n")
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					line = strings.TrimSpace(line)
					switch {
					case strings.HasPrefix(line, "EHLO"):
						if encrypted {
							_, _ = io.WriteString(conn, "250-local\r\n250 AUTH PLAIN\r\n")
						} else {
							_, _ = io.WriteString(conn, "250-local\r\n250 STARTTLS\r\n")
						}
					case line == "STARTTLS":
						_, _ = io.WriteString(conn, "220 begin TLS\r\n")
						conn = tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{certificate}})
						reader = bufio.NewReader(conn)
						encrypted = true
					case strings.HasPrefix(line, "AUTH PLAIN "):
						if !encrypted {
							return
						}
						authed <- struct{}{}
						_, _ = io.WriteString(conn, "235 authenticated\r\n")
					case line == "DATA":
						_, _ = io.WriteString(conn, "354 send message\r\n")
						for {
							data, err := reader.ReadString('\n')
							if err != nil {
								return
							}
							if data == ".\r\n" {
								break
							}
						}
						_, _ = io.WriteString(conn, "250 queued\r\n")
					case line == "QUIT":
						_, _ = io.WriteString(conn, "221 bye\r\n")
						return
					default:
						_, _ = io.WriteString(conn, "250 ok\r\n")
					}
				}
			})
			requestCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := smtpExchange(requestCtx, net.JoinHostPort(host, strconv.Itoa(port)), &tls.Config{ServerName: host, RootCAs: pool}, implicit, smtp.PlainAuth("", "user", "test-secret", host), "sender@example.test", []string{"to@example.test"}, []byte("Subject: test\r\n\r\nnotification\r\n"))
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-authed:
			default:
				t.Fatal("authentication skipped")
			}
		})
	}
}
