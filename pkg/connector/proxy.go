package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"maunium.net/go/mautrix"
)

// TODO move proxy stuff to mautrix-go

type respGetProxy struct {
	ProxyURL string `json:"proxy_url"`
}

func (wa *WhatsAppConnector) getProxy(reason string) (string, error) {
	if wa.Config.GetProxyURL == "" {
		return wa.Config.Proxy, nil
	}
	parsed, err := url.Parse(wa.Config.GetProxyURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse address: %w", err)
	}
	q := parsed.Query()
	q.Set("reason", reason)
	parsed.RawQuery = q.Encode()
	req, err := http.NewRequest(http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", fmt.Errorf("failed to prepare request: %w", err)
	}
	req.Header.Set("User-Agent", mautrix.DefaultUserAgent)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	} else if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		return "", fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	var respData respGetProxy
	err = json.NewDecoder(resp.Body).Decode(&respData)
	if err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}
	return respData.ProxyURL, nil
}

// versionProxy is how the latest-version check's request goes out: through the
// static `proxy`, so a bridge whose only way out is that proxy (Nara's chat
// enclave, which has no DNS) still learns the latest WhatsApp web version. Nil
// (direct, as before) with none, with proxy_only_login, or with a scheme
// SetProxyAddress wouldn't take either. It doesn't ask get_proxy_url.
func (wa *WhatsAppConnector) versionProxy() func(*http.Request) (*url.URL, error) {
	if wa.Config.Proxy == "" || wa.Config.ProxyOnlyLogin {
		return nil
	}
	parsed, err := url.Parse(wa.Config.Proxy)
	if err != nil {
		wa.Bridge.Log.Err(err).Msg("Failed to parse proxy address for the version check")
		return nil
	}
	switch parsed.Scheme {
	case "http", "https", "socks5":
		return http.ProxyURL(parsed)
	default:
		wa.Bridge.Log.Warn().Str("scheme", parsed.Scheme).Msg("Unsupported proxy scheme for the version check, going direct")
		return nil
	}
}

func (wa *WhatsAppConnector) updateProxy(ctx context.Context, client *whatsmeow.Client, isLogin bool) error {
	if wa.Config.ProxyOnlyLogin && !isLogin {
		return nil
	}
	reason := "connect"
	if isLogin {
		reason = "login"
	}
	if proxy, err := wa.getProxy(reason); err != nil {
		return fmt.Errorf("failed to get proxy address: %w", err)
	} else if proxy == "" {
		return nil
	} else if err = client.SetProxyAddress(proxy, whatsmeow.SetProxyOptions{
		OnlyLogin: wa.Config.ProxyOnlyLogin,
		NoMedia:   wa.Config.ProxyOnlyLogin,
	}); err != nil {
		return fmt.Errorf("failed to set proxy address: %w", err)
	}
	zerolog.Ctx(ctx).Debug().Msg("Enabled proxy")
	return nil
}
