package codecs

import (
	"encoding/json"

	"github.com/spartanlogs/spartan/codecs"
	"github.com/spartanlogs/spartan/config"
	"github.com/spartanlogs/spartan/event"
	"github.com/spartanlogs/spartan/utils"
)

type jsonLineConfig struct {
	delimiter string
}

var jsonLineConfigSchema = []config.Setting{
	{
		Name:    "delimiter",
		Type:    config.String,
		Default: "\n",
	},
}

// The JSONLineCodec encodes/decodes an event as JSON.
type JSONLineCodec struct {
	codecs.BaseCodec
	config *jsonLineConfig
}

func init() {
	codecs.Register("json_lines", newJSONLineCodec)
}

func newJSONLineCodec(options utils.InterfaceMap) (codecs.Codec, error) {
	c := &JSONLineCodec{
		config: &jsonLineConfig{},
	}
	return c, c.setConfig(options)
}

func (c *JSONLineCodec) setConfig(options utils.InterfaceMap) error {
	var err error
	options, err = config.VerifySettings(options, jsonLineConfigSchema)
	if err != nil {
		return err
	}

	c.config.delimiter = options.Get("delimiter").(string)

	return nil
}

// Encode Event as JSON object.
func (c *JSONLineCodec) Encode(e *event.Event) []byte {
	data := e.Data()
	j, _ := json.Marshal(data)
	return append(j, []byte(c.config.delimiter)...)
}

// Decode byte slice into an Event. CURRENTLY NOT IMPLEMENTED.
func (c *JSONLineCodec) Decode(data []byte) (*event.Event, error) {
	return nil, nil
}
