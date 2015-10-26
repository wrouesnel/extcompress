package extcompress

import (
    "testing"
    "io"
    "io/ioutil"
    "os"
    "path"
    "github.com/stretchr/testify/assert"
	"bytes"
	"fmt"
	"strings"
	"path/filepath"
	"github.com/Sirupsen/logrus"
)

const data = `
this is some non-random data we'll feed to an external compression function.
`

func setupTestDir(t *testing.T) string {
	logrus.SetLevel(logrus.DebugLevel)

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

// Test mime handlers
func TestMimeHandlerMappings(t *testing.T) {
	tmpdir := setupTestDir(t)
	defer os.RemoveAll(tmpdir)
	fmt.Println(tmpdir)

	CheckHandlers()

	// Helper to check mimetype logic
	mimeCheck := func (hSource ExternalHandler, hResult ExternalHandler) {
		// empty handling actually results in text
		fmt.Println(hSource.MimeType(), hResult.MimeType())
		assert.EqualValues(t, mimeMap[hSource.MimeType()], mimeMap[hResult.MimeType()])
	}

	// Helper to find altered in-place filenames
	globMatch := func (filename string) string {
		s, _ := filepath.Glob(fmt.Sprintf("%s*", filename))
		return s[0]
	}

	// Basic sanity
	for k, _ := range mimeMap {
		fmt.Println("Checking", k)
		h, err := GetExternalHandlerFromMimeType(k)
		assert.Nil(t, err)
		assert.Equal(t, k, h.MimeType())

		filename := path.Join(tmpdir,strings.Replace(k, "/", "_", -1))

		srctext := []byte("this is some text\n")
		b := bytes.NewBuffer(srctext)
		r, err := h.CompressStream(b)
		assert.Nil(t, err)
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(0777))
		assert.Nil(t, err)
		io.Copy(f, r)
		f.Sync()
		f.Close()
		assert.Zero(t, r.Result())

		hr, err := GetFileTypeExternalHandler(filename)
		assert.Nil(t, err)
		mimeCheck(h, hr)

		// Test streaming decompression
		dr, err := h.Decompress(filename)
		assert.Nil(t, err)
		br, err := ioutil.ReadAll(dr)
		assert.Nil(t, err)
		assert.EqualValues(t, srctext, br)
		assert.Zero(t, dr.Result())

		// Setup for in-place tests
		err = ioutil.WriteFile(filename, srctext, os.FileMode(0777))
		assert.Nil(t, err)

		// Test in-place compression
		err = h.CompressFileInPlace(filename) // Recompress
		assert.Nil(t, err)

		mutatedFilename := globMatch(filename)
		fmt.Println("Looking for mutated filename: ", mutatedFilename)
		h_inplace, _ := GetFileTypeExternalHandler(mutatedFilename) // Should be remutated
		mimeCheck(h, h_inplace)

		// Test in-place decompression
		err = h.DecompressFileInPlace(mutatedFilename)
		assert.Nil(t, err)
		hfinal, _ := GetFileTypeExternalHandler(filename) // Should now be filename
		assert.Equal(t, "text/plain", hfinal.MimeType())
	}
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