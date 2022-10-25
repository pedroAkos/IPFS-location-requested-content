package db

type Config interface {
}

type PostgresConf struct {
	Host     string
	Port     int
	User     string
	Password string
	DBname   string
}

type InfluxDBConf struct {
	Org    string
	Bucket string
	DBUrl  string
	Token  string
}
