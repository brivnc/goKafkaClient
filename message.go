package kafkaclient

type Message struct {
	User    string `json:"user"`
	Message string `json:"message"`
}
