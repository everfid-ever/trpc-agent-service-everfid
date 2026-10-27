package inflight

import (
	"net/http"
	"strconv"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// ModelTransport pauses only after the real upstream has returned response
// headers, before the SDK can consume the model result. No fake response is
// substituted and no recovery or replay is performed by the transport.
type ModelTransport struct{ Base http.RoundTripper }

func (t ModelTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := runtime.ObserveLLMRequest(request.Context()); err != nil {
		return nil, err
	}
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	response, err := base.RoundTrip(request)
	if err != nil {
		return response, err
	}
	if response != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
		ctx := request.Context()
		identity, _ := ctx.Value(scopeKey{}).(scope)
		ctx = WithScope(ctx, identity.TenantID, identity.RequestID, "model_http_status:"+strconv.Itoa(response.StatusCode))
		if err = Wait(ctx, PointP2DuringModelResponse); err != nil {
			if response.Body != nil {
				response.Body.Close()
			}
			return nil, err
		}
	}
	return response, nil
}
