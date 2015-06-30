package extcompress

import (
    "testing"
    "io"
    "io/ioutil"
    "os"
    "path"
    "github.com/stretchr/testify/assert"
)

const data = `
this is some non-random data we'll feed to an external compression function.
`

func TestPipeChaining(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "extcompress_test")
	assert.Nil(t, err)
	start := path.Join(tmpdir, "pipechaining")
	ioutil.WriteFile(start, []byte(data), os.FileMode(777))
	
	h, err := GetExternalHandlerFromMimeType("text/plain")
	assert.Nil(t, err)
	
	mh, err := GetExternalHandlerFromMimeType("application/x-bzip2")
	assert.Nil(t, err)
	
	fh, err := ioutil.TempFile(tmpdir, "outfile")
	assert.Nil(t, err)
	
	start_r, err := h.Decompress(start)
	assert.Nil(t, err)
	
	mr, err := mh.CompressStream(start_r)
	assert.Nil(t, err)
	
	_, err = io.Copy(fh, mr)
	assert.Nil(t, err)
	
	os.RemoveAll(tmpdir)
}

