package writer

import (
	"fmt"
)

type ErrBacklogOverflow int

func (e ErrBacklogOverflow) Error() string {
	return fmt.Sprintf(
		"No free slots available in writer backlog(slow writer?), total '%d'",
		e,
	)
}

func NewErrBacklogOverflow(slots int) ErrBacklogOverflow {
	return ErrBacklogOverflow(slots)
}
