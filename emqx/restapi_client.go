package emqx

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

// RestAPIClientConfig client config
type RestAPIClientConfig struct {
	// EMQX API base url, default to http://localhost:18083
	BaseURL string
	// EMQX Application ID
	AppID string
	// EMQX Application Secret
	AppSecret string
	// EMQX client timeout
	Timeout time.Duration
}

// RestAPIClient EMQX RESTFul API client
type RestAPIClient struct {
	// BaseURL emqx RESTFul address
	BaseURL    string
	httpClient *http.Client
	appID      string
	appSecret  string
	token      string
}

// NewAPIClient create client
func NewAPIClient(c RestAPIClientConfig) *RestAPIClient {
	a := &RestAPIClient{
		httpClient: http.DefaultClient,
		appID:      c.AppID,
		appSecret:  c.AppSecret,
	}

	if c.BaseURL == "" {
		c.BaseURL = "http://localhost:18083"
	}
	a.BaseURL = c.BaseURL

	if c.Timeout == 0 {
		c.Timeout = time.Second * 5
	}
	a.httpClient.Timeout = c.Timeout

	a.updateToken(c.AppID, c.AppSecret)

	return a
}

// UpdateToken update token with appID and appSecret
func (a *RestAPIClient) updateToken(appID, appSecret string) {
	str := fmt.Sprintf("%s:%s", appID, appSecret)
	a.token = fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(str)))
}

// makeRequest makeRequest
func (a *RestAPIClient) makeRequest(method, endpoint string, payload []byte) (ret string, err error) {
	url := fmt.Sprintf("%s/%s", a.BaseURL, endpoint)
	var body io.Reader
	if payload != nil {
		body = bytes.NewBuffer(payload)
	}
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		return
	}

	request.Header = http.Header{
		"Authorization": []string{a.token},
		"Content-Type":  []string{"application/json"},
	}

	response, err := a.httpClient.Do(request)
	if err != nil {
		return
	}

	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}
	defer response.Body.Close()
	ret = string(content)
	return
}

// GET api/v5/status
func (a *RestAPIClient) Status() (resp string, err error) {
	return a.makeRequest(http.MethodGet, "api/v5/status", nil)
}

func (a *RestAPIClient) Topics() (resp string, err error) {
	return a.makeRequest(http.MethodGet, "api/v5/topics", nil)
}
