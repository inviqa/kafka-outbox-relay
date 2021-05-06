package job

import "inviqa/kafka-outbox-relay/log"

type SidecarQuitter struct {
	QuitSidecar     bool
	Client          httpPoster
	sidecarProxyUrl string
}

func (s *SidecarQuitter) EnableSideCarProxyQuit(proxyUrl string) {
	s.QuitSidecar = true
	s.sidecarProxyUrl = proxyUrl
}

func (s *SidecarQuitter) Quit() error {
	_, err := s.Client.Post(s.sidecarProxyUrl+"/quitquitquit", "text/plain", nil)
	if err != nil {
		log.Logger.WithError(err).Error("unexpected error received from sidecar proxy /quitquitquit")
		return err
	}

	return nil
}
