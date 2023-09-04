package historyinfo

import "time"

type TrackedVar struct {
	version    int
	updateTime time.Time
	value      interface{}
}

func NewTrackedVar(value interface{}) *TrackedVar {
	return &TrackedVar{
		version:    0,
		updateTime: time.Now(),
		value:      value,
	}
}

func NewNextTrackedVar(tv *TrackedVar, value interface{}) *TrackedVar {
	return &TrackedVar{
		version:    tv.version + 1,
		updateTime: time.Now(),
		value:      value,
	}
}

func NewInfoWithVersionAndTime(version int, updateTime time.Time, value interface{}) *TrackedVar {
	return &TrackedVar{
		version:    version,
		updateTime: updateTime,
		value:      value,
	}
}

func (this *TrackedVar) GreaterThan(info *TrackedVar) bool {
	if this.version > info.version {
		return true
	}

	if this.version < info.version {
		return false
	}

	return this.updateTime.After(info.updateTime)
}
