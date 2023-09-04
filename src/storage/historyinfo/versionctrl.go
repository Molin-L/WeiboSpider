package historyinfo

type VersionCtrl struct {
	trackedVars []TrackedVar
}

func NewVersionCtrl() *VersionCtrl {
	return &VersionCtrl{
		trackedVars: make([]TrackedVar, 0),
	}
}

func (this *VersionCtrl) Update(key string, value interface{}) {
	tv := this.GetLatest()
	if tv != nil && tv.value == value {
		return
	}

	this.Append(NewNextTrackedVar(tv, value))
}

// GetLatest returns the latest version info
func (this *VersionCtrl) GetLatest() *TrackedVar {
	if len(this.trackedVars) == 0 {
		return nil
	}
	return &this.trackedVars[len(this.trackedVars)-1]
}

func (this *VersionCtrl) Append(tv *TrackedVar) *VersionCtrl {
	this.trackedVars = append(this.trackedVars, *tv)
	return this
}
