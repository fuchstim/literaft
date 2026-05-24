package logger

type Params struct {
	Level       string `config:"name='log.level',default=info,usage='The logging level.'"`
	Development bool   `config:"name='log.development',default=false,usage='Whether to use development logging settings.'"`
}
