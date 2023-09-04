package historyinfo

import "strconv"

type Profile struct {
	Id            int64
	UserName      VersionCtrl
	FollowCount   VersionCtrl
	FollowerCount VersionCtrl
	ProfileUrl    string
	Gender        VersionCtrl
	Description   VersionCtrl
	PostsCount    VersionCtrl

	// Image
	AvatarUrl     VersionCtrl
	CoverImageUrl VersionCtrl
}

func NewProfile(userInfo map[string]interface{}) *Profile {
	followerCount, _ := strconv.ParseInt((userInfo["followers_count_str"].(string)), 10, 64)
	return &Profile{
		Id:            int64(userInfo["id"].(float64)),
		UserName:      *(NewVersionCtrl().Append(NewTrackedVar(userInfo["screen_name"]))),
		FollowCount:   *(NewVersionCtrl().Append(NewTrackedVar(int64(userInfo["follow_count"].(float64))))),
		FollowerCount: *(NewVersionCtrl().Append(NewTrackedVar(followerCount))),
		PostsCount:    *(NewVersionCtrl().Append(NewTrackedVar(int64(userInfo["statuses_count"].(float64))))),
		ProfileUrl:    userInfo["profile_url"].(string),
		Gender:        *(NewVersionCtrl().Append(NewTrackedVar(userInfo["gender"]))),
		Description:   *(NewVersionCtrl().Append(NewTrackedVar(userInfo["description"]))),
		AvatarUrl:     *(NewVersionCtrl().Append(NewTrackedVar(userInfo["avatar_hd"]))),
		CoverImageUrl: *(NewVersionCtrl().Append(NewTrackedVar(userInfo["cover_image_phone"]))),
	}
}
