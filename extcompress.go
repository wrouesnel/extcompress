/*
	package which provides a set of helpers to wrap external compression
	commands behind writer/reader interfaces.
*/

package extcompress

import (
	"os/exec"
	"io"
	"github.com/rakyll/magicmime"
	
	log "github.com/Sirupsen/logrus"
)

// Map of stream compressors
var filtersMap map[string]string = map[string]string{
	"application/x-bzip2" : "bzip2",
	"application/gzip" : "gzip",
	"application/x-xz" : "xz",
	"text/plain" : "cat",
}

// Do a filemagic lookup and return a handler interface for the given type
func GetFileTypeExternalHandler(filePath string) (ExternalHandler, error) {
	mm, err := magicmime.New(magicmime.MAGIC_MIME_TYPE | 
		magicmime.MAGIC_SYMLINK | magicmime.MAGIC_ERROR)
    if err != nil {
        return nil, err
    }

    mimetype, err := mm.TypeByFile(filePath)
    if err != nil {
        return nil, err
    }
    
    return GetExternalHandlerFromMimeType(mimetype)
}

func GetExternalHandlerFromMimeType(mimeType string) (ExternalHandler, error) {
	handler, ok := filtersMap[mimeType]
    if !ok {
    	return nil, error(UnknownFileType{})
    }
    
    extHandler := ExternalHandler(Filter{handler})
    return extHandler, nil
}

type UnknownFileType struct {}
func (r UnknownFileType) Error() string {
	return "This file type is not known to us."
}

// Interface of an external handler type for dealing with library compression
type ExternalHandler interface {
	// Stream compression/decompression from file
	Compress(filePath string) (io.ReadCloser, error)
	Decompress(filePath string) (io.ReadCloser, error)
	
	// Pure stream handlers
	CompressStream(io.ReadCloser) (io.ReadCloser, error)
	DecompressStream(io.ReadCloser) (io.ReadCloser, error)
	
	// In place compression/decompression
	CompressFileInPlace(filePath string) error
	DecompressFileInPlace(filePath string) error
	
	// Informational - return the commands this interface will run as strings
	CommandCompress() string
	CommandDecompress() string
}

// Handles the unix-style filter commands
type Filter struct {
	Command string
}

func (c Filter) CommandCompress() string {
	return c.Command + " -c"
}

func (c Filter) CommandDecompress() string {
	return c.Command + "-d -c"
}

func (c Filter) Compress(filePath string) (io.ReadCloser, error) {
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command,filePath)
	err := cmd.Start()
	if err != nil {
		log.WithFields(logFields).Error("Compression command failed.")
		return nil, err
	}
	
	log.Debug("External compression finished successfully.")
	return cmd.StdoutPipe()
}

func (c Filter) CompressStream(rd io.ReadCloser) (io.ReadCloser, error) {
	var logFields = log.Fields{"compressCmd" : c.Command }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command,"-c")
	cmd.Stdin = rd
	err := cmd.Start()
	if err != nil {
		log.WithFields(logFields).Error("Compression command failed.")
		return nil, err
	}
	
	log.Debug("External compression finished successfully.")
	return cmd.StdoutPipe()
}

// Call the compression utility in standalone compression mode
func (c Filter) CompressFileInPlace(filePath string) error {	
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command,filePath)
	err := cmd.Run()
	if err != nil {
		log.WithFields(logFields).Warn("Compression command failed.")
	}
	
	log.Debug("External compression finished successfully.")
	return err
}

func (c Filter) DecompressStream(rd io.ReadCloser) (io.ReadCloser, error) {
	var logFields = log.Fields{"compressCmd" : c.Command }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command,"-d","-c")
	cmd.Stdin = rd
	err := cmd.Start()
	if err != nil {
		log.WithFields(logFields).Error("Compression command failed.")
		return nil, err
	}
	
	log.Debug("External compression finished successfully.")
	return cmd.StdoutPipe()
}

func (c Filter) DecompressFileInPlace(filePath string) error {	
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Decompression Command")
	
	cmd := exec.Command(c.Command, "-d", filePath)
	err := cmd.Run()
	if err != nil {
		log.WithFields(logFields).Warn("DeCompression command failed.")
	}
	
	log.Debug("External compression finished successfully.")
	return err
}

// Decompress the given file and return the stream
func (c Filter) Decompress(filePath string) (io.ReadCloser, error) {
	cmd := exec.Command(c.Command, "-d", filePath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("External decompression command error: %s", err.Error())
		return nil, err
	}
	
	if err := cmd.Start(); err != nil {
		log.Errorf("External decompression command error:", err.Error())
		return nil, err
	}
	
	return stdout, nil
}