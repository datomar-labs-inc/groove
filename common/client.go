package groove

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

type Client struct {
	baseURL string
	client  *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 50 * time.Second,
		},
	}
}

type EnqueueResponse struct {
	Enqueued int    `json:"enqueued"`
	Status   string `json:"status"`
}

func (c *Client) Enqueue(ctx context.Context, tasks []Task) (*EnqueueResponse, error) {
	jsb, err := json.Marshal(EnqueueTaskInput{Tasks: tasks})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx,"POST", fmt.Sprintf("%s/enqueue", c.baseURL), bytes.NewReader(jsb))
	if err != nil {
		return nil, err
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, errors.New(string(body))
	}

	var response EnqueueResponse

	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

type DequeueResponse struct {
	Status  string  `json:"status"`
	TaskSet TaskSet `json:"task_set"`
}

func (c *Client) Dequeue(ctx context.Context, input DequeueTaskInput) (*DequeueResponse, error) {
	jsb, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/dequeue", c.baseURL), bytes.NewReader(jsb))
	if err != nil {
		return nil, err
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, errors.New(string(body))
	}

	var response DequeueResponse

	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

type AckResponse struct {
	Status string `json:"status"`
}

func (c *Client) Ack(ctx context.Context, input AckInput) (*AckResponse, error) {
	jsb, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/ack", c.baseURL), bytes.NewReader(jsb))
	if err != nil {
		return nil, err
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, errors.New(string(body))
	}

	var response AckResponse

	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

func (c *Client) Nack(ctx context.Context, input AckInput) (*AckResponse, error) {
	jsb, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/nack", c.baseURL), bytes.NewReader(jsb))
	if err != nil {
		return nil, err
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, errors.New(string(body))
	}

	var response AckResponse

	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}


