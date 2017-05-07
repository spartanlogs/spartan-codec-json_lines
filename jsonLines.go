package codecs

import (
	"encoding/json"
	"io"

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
	if err := config.VerifySettings(options, jsonLineConfigSchema); err != nil {
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

// EncodeWriter reads events from in and writes them to w
func (c *JSONLineCodec) EncodeWriter(w io.Writer, in <-chan *event.Event) {}

// Decode byte slice into an Event. CURRENTLY NOT IMPLEMENTED.
func (c *JSONLineCodec) Decode(data []byte) (*event.Event, error) {
	return nil, nil
}

// DecodeReader reads from r and creates an event sent to out
func (c *JSONLineCodec) DecodeReader(r io.Reader, out chan<- *event.Event) {}
