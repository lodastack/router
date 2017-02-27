package models

type Point struct {
	Measurement string                 `json:"measurement"`
	Timestamp   int64                  `json:"timestamp"`
	Tags        map[string]string      `json:"tags"`
	Fields      map[string]interface{} `json:"fields"`
}

type Points struct {
	Precision       string   `json:"precision"`
	Database        string   `json:"database"`
	RetentionPolicy string   `json:"retentionPolicy"`
	Points          []*Point `json:"points"`
}
