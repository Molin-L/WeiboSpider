package historyinfo

import (
	"strconv"
	"sync"
	"time"

	"weibospider/src/dto"
)

const layout = "Mon Jan 02 15:04:05 -0700 2006"

type Post struct {
	lock sync.Mutex
	mes
	Uid        string
	ScreenName string
	PostId     string
	Content    VersionCtrl
	Location   string
	CreateAt   time.Time
	Device     string
	PostUrl    string
	LikeCount  VersionCtrl
	Comments   []dto.Comment
	AtUsers    []Profile

	// Retweet Post
	IsRetweet bool
	Origin    *Post

	// Image
	Images      []string
	LocalImages map[string]string
}

func ParsePost(data map[string]interface{}) *Post {
	if data["card_type"] != "9" {
		return nil
	}
	post_data := data["mblog"].(map[string]interface{})

	time_str := post_data["created_at"].(string)
	createAt, _ := time.Parse(layout, time_str)
	// Time format: Sun Sep 03 12:55:42 +0800 2023
	// Todo: 处理评论
	post := &Post{
		Uid:       strconv.Itoa(int(post_data["user"].(map[string]interface{})["id"].(float64))),
		PostUrl:   data["scheme"].(string),
		PostId:    post_data["id"].(string),
		Device:    post_data["source"].(string),
		Content:   *(NewVersionCtrl().Append(NewTrackedVar(post_data["text"]))),
		CreateAt:  createAt,
		LikeCount: *(NewVersionCtrl().Append(NewTrackedVar(int64(post_data["attitudes_count"].(float64))))),
		IsRetweet: false,
		Origin:    nil,
	}

	// 转发微博
	if post_data["retweeted_status"] != nil {
		post.IsRetweet = true
		return post
	} else {
		// 原创微博
		if post_data["isLongText"] == true {
			// Todo: Process Long Text
			return post
		}
	}
	return post
}
