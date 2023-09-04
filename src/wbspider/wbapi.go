package wbspider

import (
	"context"
	"encoding/json"
	"github.com/Afterlife-Quant/golib/pkg/log"
	"go.uber.org/zap"
	"net/http"
	"strconv"

	"weibospider/src/storage/historyinfo"
)

const (
	wb_url = "https://m.weibo.cn/api/container/getIndex?"
)

func getJSON(params map[string]string) (map[string]interface{}, error) {
	req, err := http.NewRequest("GET", wb_url, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	for key, value := range params {
		q.Add(key, value)
	}
	req.URL.RawQuery = q.Encode()

	log.Info(context.Background(), "", zap.String("request url:", req.URL.String()))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func GetUserProfile(uid string) *historyinfo.Profile {
	log.Info(context.Background(), "Get user profile", zap.String("uid", uid))
	params := map[string]string{
		"containerid": "100505" + uid,
	}
	jsonData, err := getJSON(params)

	if err != nil {
		log.Error(context.Background(), err.Error())
		return nil
	}

	return historyinfo.NewProfile(jsonData["data"].(map[string]interface{})["userInfo"].(map[string]interface{}))
}

func GetWbPage(uid string, page int) []*historyinfo.Post {
	log.Info(context.Background(), "Get wb page", zap.String("uid", uid), zap.Int("page", page))
	params := map[string]string{
		"containerid": "107603" + uid,
		"page":        strconv.Itoa(page),
	}

	jsonData, err := getJSON(params)
	if err != nil {
		log.Error(context.Background(), err.Error())
		return nil
	}

	if jsonData["ok"].(int) != 1 {
		log.Error(context.Background(), "Get wb page failed", zap.String("uid", uid), zap.Int("page", page))
		return nil
	}

	return nil
}
