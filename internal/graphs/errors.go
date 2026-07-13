package graphs

// UserBadRequestError represents an error that we want to surface to the user as an HTTP-400.
type UserBadRequestError string

func (e UserBadRequestError) Error() string {
	return string(e)
}
