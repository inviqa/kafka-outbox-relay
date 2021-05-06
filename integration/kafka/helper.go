// +build integration

package kafka

func GetTopicsFromMessageExpectations(msgs []MessageExpectation) []string {
	seen := map[string]bool{}
	var topics []string
	for _, m := range msgs {
		if !seen[m.Msg.Topic] {
			seen[m.Msg.Topic] = true
			topics = append(topics, m.Msg.Topic)
		}
	}
	return topics
}
