package gh

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/githubauth"
)

func TestListRunnersSameScopePrefixesDoNotOverlap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_count":2,"runners":[{"id":1,"name":"gh-runner-a-10000"},{"id":2,"name":"gh-runner-a-b-11000"}]}`))
	}))
	defer server.Close()
	cli := newTestClient(t, server)
	scope := githubauth.Scope{Org: "example"}
	a, err := ListRunnersByPrefix(context.Background(), cli, scope, "gh-runner-a-", nil)
	require.NoError(t, err)
	require.Contains(t, a, "gh-runner-a-10000")
	require.NotContains(t, a, "gh-runner-a-b-11000")
	b, err := ListRunnersByPrefix(context.Background(), cli, scope, "gh-runner-a-b-", nil)
	require.NoError(t, err)
	require.Contains(t, b, "gh-runner-a-b-11000")
	require.NotContains(t, b, "gh-runner-a-10000")
}
