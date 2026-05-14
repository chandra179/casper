package broker

type Config struct {
	URL      string `yaml:"url"`
	Exchange string `yaml:"exchange"`
	Prefetch int    `yaml:"prefetch"`
}
