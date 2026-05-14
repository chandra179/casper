package broker

type Config struct {
	URI      string `yaml:"uri"`
	URL      string `yaml:"url"`
	Exchange string `yaml:"exchange"`
	Prefetch int    `yaml:"prefetch"`
}

func (c Config) ConnectionURL() string {
	if c.URI != "" {
		return c.URI
	}
	return c.URL
}
