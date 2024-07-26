package tests

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	gods "github.com/deltastreaminc/go-deltastream"
)

const tt = `eyJhbGciOiJFUzI1NiIsImtpZCI6MTEsInR5cCI6IkpXVCJ9.eyJpc3MiOiJkZWx0YXN0cmVhbS5pbyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MTAxNzYxODg4ODIzMDQ0ODQzNjAxIiwiZXhwIjoxNzI5NzIwODQ4LCJpYXQiOjE3MjE5NDQ4NDgsImp0aSI6IjNlZTA1MjQzLWU0ZmQtNGE0NS04NDZiLWJkM2VlOTUwOTM5YiIsInRyYWNlaWQiOiJmOTMyYmUzMGY4MTY2ZTZjZTg0MGE2NTliYThkMDZiNyIsInN0YXRlbWVudElEIjoiMDAwMDAwMDAtMDAwMC0wMDAwLTAwMDAtMDAwMDAwMDAwMDAwIn0.pQfQ_C0Hz2SNU7ICaeGFt2ekYo9zbS40cXwsEn6mLsrofXxWRbn0jGyFUFMVkoqdbNipos1NJTOvnYTwxEXJBg`

type debugTransport struct {
	r      http.RoundTripper
	stderr io.Writer
}

func (d *debugTransport) RoundTrip(h *http.Request) (*http.Response, error) {
	dump, _ := httputil.DumpRequestOut(h, true)
	fmt.Fprintf(d.stderr, "request: %s\n", string(dump))
	resp, err := d.r.RoundTrip(h)
	if resp != nil {
		dump, _ = httputil.DumpResponse(resp, true)
		fmt.Fprintf(d.stderr, "response: %s\n", string(dump))
	} else {
		fmt.Fprintf(d.stderr, "response is nil\n")
	}
	return resp, err
}

func TestTimeout(t *testing.T) {
	g := NewGomegaWithT(t)
	ctx := context.Background()

	var wait sync.WaitGroup
	for i := 0; i < 20; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()

			for j := 0; j < 10; j++ {
				client := &http.Client{
					Timeout: 10 * time.Second,
					Transport: &debugTransport{
						r:      http.DefaultTransport,
						stderr: os.Stderr,
					},
				}

				connOptions := []gods.ConnectionOption{
					gods.WithHTTPClient(client),
					// gods.WithColumnDisplayHints(),
					gods.WithStaticToken(tt),
					gods.WithSessionID("rgc-testing2"),
					gods.WithServer("https://api.stage.deltastream.io/v2"),
				}

				connector, err := gods.ConnectorWithOptions(ctx, connOptions...)
				if err != nil {
					g.Expect(err).To(BeNil())
				}

				db := sql.OpenDB(connector)
				conn, err := db.Conn(ctx)
				if err != nil {
					g.Expect(err).To(BeNil())
				}

				err = db.PingContext(ctx)
				g.Expect(err).To(BeNil())

				_, err = conn.ExecContext(ctx, "use organization 0f0dfddd-3339-4388-8884-5cac6063c148;")
				g.Expect(err).To(BeNil())
				_, err = conn.ExecContext(ctx, "use store mystore;")
				g.Expect(err).To(BeNil())
				_, err = conn.ExecContext(ctx, "use role sysadmin;")
				g.Expect(err).To(BeNil())
				_, err = conn.ExecContext(ctx, "show regions;")
				g.Expect(err).To(BeNil())
				defer conn.Close()
			}

		}()
	}
	wait.Wait()

	t.Log("Failing test")
	g.Expect(false).To(BeTrue())
}
