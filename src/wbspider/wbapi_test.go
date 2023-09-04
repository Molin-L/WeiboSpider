package wbspider

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestGetProfile(t *testing.T) {
	uid := "1402400261"
	profile := GetUserProfile(uid)
	assert.Equal(t, strconv.Itoa(int(profile.Id)), uid)
}

func TestGetWbPage(t *testing.T) {
	uid := "1402400261"
	GetWbPage(uid, 1)
}
