package prometheus

type Sizer interface {
	GetQueueSize() (uint, error)
	GetTotalSize() (uint, error)
}
