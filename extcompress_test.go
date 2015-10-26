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

func setupTestDir(t *testing.T) string {
	tmpdir, err := ioutil.TempDir("", "extcompress_test")
	assert.Nil(t, err)
	start := path.Join(tmpdir, "pipechaining")
	ioutil.WriteFile(start, []byte(data), os.FileMode(777))
	return tmpdir
}

func TestPipeChaining(t *testing.T) {
	tmpdir := setupTestDir(t)
	defer os.RemoveAll(tmpdir)
	
	h, err := GetExternalHandlerFromMimeType("text/plain")
	assert.Nil(t, err)
	
	mh, err := GetExternalHandlerFromMimeType("application/x-bzip2")
	assert.Nil(t, err)
	
	fh, err := ioutil.TempFile(tmpdir, "outfile")
	assert.Nil(t, err)
	
	start_r, err := h.Decompress(path.Join(tmpdir, "pipechaining"))
	assert.Nil(t, err)
	
	mr, err := mh.CompressStream(start_r)
	assert.Nil(t, err)
	
	_, err = io.Copy(fh, mr)
	assert.Nil(t, err)

	//fh.Close()
	//start_r.Close()
	//mr.Close()

	// Check job results
	assert.Zero(t, start_r.Result())
	assert.Zero(t, mr.Result())
}

//func TestEarlyPipeClose(t *testing.T) {
//	tmpdir := setupTestDir(t)
//	defer os.RemoveAll(tmpdir)
//
//	h, err := GetExternalHandlerFromMimeType("text/plain")
//	assert.Nil(t, err)
//
//	mh, err := GetExternalHandlerFromMimeType("application/x-bzip2")
//	assert.Nil(t, err)
//
//	fh, err := ioutil.TempFile(tmpdir, "outfile")
//	assert.Nil(t, err)
//
//	start_r, err := h.Decompress(start)
//	assert.Nil(t, err)
//
//	mr, err := mh.CompressStream(start_r)
//	assert.Nil(t, err)
//
//	_, err = io.Copy(fh, mr)
//	assert.Nil(t, err)
//}

//func TestFailedCopy(t *testing.T) {
//	tmpdir, err := ioutil.TempDir("", "brokenpipe_test")
//	assert.Nil(t, err)
//
//	start := path.Join(tmpdir, "brokenpipe")
//	ioutil.WriteFile(start, []byte(data), os.FileMode(777))
//
//	h, err := GetExternalHandlerFromMimeType("text/plain")
//	assert.Nil(t, err)
//
//	fh, err := ioutil.TempFile(tmpdir, "outfile")
//	assert.Nil(t, err)
//
//	fh.WriteString("qdejoiqedjqeoidjewfhwiufhwrifundwcijnerfiuvhwdfcjwncdiweuc")
//	fh.Seek(0, os.SEEK_SET)
//
//	mr, err := h.CompressStream(fh)
//	assert.Nil(t, err)
//}