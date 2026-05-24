package adminapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// FuzzHandleDestroyVMID drives the /admin/destroy/{vmid} route with
// adversarial path segments via the same chi router production uses.
// The handler reads chi.URLParam(r, "vmid") and runs strconv.Atoi over
// it — a panic anywhere in that chain would crash the leader on a
// malformed admin request.
//
// Properties:
//
//  1. Must never panic.
//  2. Status must be 202 Accepted or 400 Bad Request — never 5xx.
//     Any input that survives the parse-and-bounds gate is a valid
//     positive integer the fake pool will accept; anything else must
//     be rejected with 400 before reaching ForceDestroy.
func FuzzHandleDestroyVMID(f *testing.F) {
	// Seeds: the positive case from TestDestroyVM_QueuesAndReturns202
	// plus the rejection cases from TestHandleDestroyVM_RejectsBadVMID,
	// then a handful of adversarial shapes called out in #142.
	f.Add("10042")
	f.Add("abc")
	f.Add("0")
	f.Add("-1")
	f.Add("9999999999999999999")
	f.Add("")
	f.Add("1\x00")
	f.Add("١٠٠٤٢") // Arabic-Indic digits — strconv.Atoi must reject
	f.Add("+10042")
	f.Add(" 10042")

	f.Fuzz(func(t *testing.T, vmid string) {
		s, _ := newTestServer(t, "topsecret")
		h := chiHandler(s)

		// PathEscape the fuzzed segment so the request URL stays
		// well-formed even when vmid contains slashes, NULs, or
		// non-ASCII — chi will URL-decode it back before URLParam.
		path := "/admin/destroy/" + url.PathEscape(vmid)
		w := httptest.NewRecorder()
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil)
		r.Header.Set("Authorization", "Bearer topsecret")
		h.ServeHTTP(w, r)

		switch w.Code {
		case http.StatusAccepted, http.StatusBadRequest, http.StatusNotFound:
			// 404 covers empty/all-slash vmids that chi can't match
			// to the route at all — also fine, not a crash.
		default:
			t.Fatalf("unexpected status %d for vmid=%q (path=%q): %s",
				w.Code, vmid, path, w.Body.String())
		}
	})
}
