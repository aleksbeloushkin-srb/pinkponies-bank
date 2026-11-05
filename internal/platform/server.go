package platform

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// Server is an http.Server whose Serve runs in a goroutine owned by the
// Server: Stop waits for it.
type Server struct {
	srv  *http.Server
	name string
	done chan struct{} // closed when Serve returns
	err  error         // Serve's result; read it only after done is closed
}

// StartServer listens on srv.Addr synchronously, so a busy port fails the
// start instead of a goroutine later, and then serves in the background.
func StartServer(name string, srv *http.Server) (*Server, error) {
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return nil, fmt.Errorf("%s listen: %w", name, err)
	}
	s := &Server{srv: srv, name: name, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		s.err = s.srv.Serve(ln)
	}()
	return s, nil
}

// Done is closed when Serve has returned, normally only after Stop.
func (s *Server) Done() <-chan struct{} { return s.done }

// Err returns Serve's result. Call it only after Done is closed.
func (s *Server) Err() error { return s.err }

// Addr returns the configured listen address.
func (s *Server) Addr() string { return s.srv.Addr }

// Stop drains the server within timeout, closes it if the drain does not
// finish, and waits until Serve has returned. The deadline is fresh: the
// signal context is already cancelled at this point, and Shutdown with a
// cancelled context would return at once without draining.
func (s *Server) Stop(ctx context.Context, timeout time.Duration) error {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	var errs []error
	if err := s.srv.Shutdown(sctx); err != nil {
		errs = append(errs, fmt.Errorf("%s shutdown: %w", s.name, err))
		_ = s.srv.Close()
	}
	<-s.done
	if s.err != nil && !errors.Is(s.err, http.ErrServerClosed) {
		errs = append(errs, fmt.Errorf("%s server: %w", s.name, s.err))
	}
	return errors.Join(errs...)
}

// Healthcheck probes /readyz on the admin listener at addr (for example
// ":9100") and returns a process exit code. Distroless images have no curl,
// so container health checks run the service binary with a "healthcheck"
// argument, which calls this.
func Healthcheck(addr string) int {
	port := addr[strings.LastIndexByte(addr, ':')+1:]
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort("127.0.0.1", port) + "/readyz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
