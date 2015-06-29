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
	"application/x-bzip2" : "pbzip2",
	"application/gzip" : "gzip",
	"application/x-xz" : "xz",
	"text/plain" : "cat",
	"application/x-empty" : "cat",
}

// Check that all handlers are properly register, fail hard if they're not.
func CheckHandlers() {
	for k, v := range filtersMap {
		hlog := log.WithField("mimetype", k).WithField("handler", v)
		_, err := exec.LookPath(v)
		if err != nil {
			hlog.Fatal("Handler unavailable!")
		}
	}
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
    
    extHandler := ExternalHandler(Filter{handler, mimeType})
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
	MimeType() string
}

// Handles the unix-style filter commands
type Filter struct {
	Command string
	mimeType string
}

func (c Filter) MimeType() string {
	return c.mimeType
}

// FIXME: Eek. There has got to be a better way to do this!
func (c Filter) CommandCompress() string {
	return c.Command + " -c"
}

func (c Filter) CommandDecompress() string {
	return c.Command + " -d -c"
}

func (c Filter) Compress(filePath string) (io.ReadCloser, error) {
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command,"-c",filePath)
	
	rdr, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("Failed to get stdout pipe.")
		return nil, err
	}
	
	err = cmd.Start()
	if err != nil {
		log.WithFields(logFields).Error("Compression command failed.")
		return nil, err
	}
	
	go func() {
		if err := cmd.Wait(); err != nil {
			log.WithField("error", err.Error()).Error("External compression command exited non-zero.")
		} else {
			log.Debug("External compression finished successfully.")
		} 
	}()
	
	return rdr, err
}

func (c Filter) CompressStream(rd io.ReadCloser) (io.ReadCloser, error) {
	var logFields = log.Fields{"compressCmd" : c.Command }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command,"-c")
	cmd.Stdin = rd
	
	rdr, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("Failed to get stdout pipe.")
		return nil, err
	}
	
	err = cmd.Start()
	if err != nil {
		log.WithFields(logFields).Error("Compression command failed.")
		return nil, err
	}
	
	go func() {
		if err := cmd.Wait(); err != nil {
			log.WithField("error", err.Error()).Error("External compression command exited non-zero.")
		} else {
			log.Debug("External compression finished successfully.")
		} 
	}()
	
	return rdr, err
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
	
	return err
}

func (c Filter) DecompressStream(rd io.ReadCloser) (io.ReadCloser, error) {
	var logFields = log.Fields{"compressCmd" : c.Command }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command,"-d","-c")
	cmd.Stdin = rd
	
	rdr, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("Failed to get stdout pipe.")
		return nil, err
	}
	
	err = cmd.Start()
	if err != nil {
		log.WithFields(logFields).Error("Compression command failed.")
		return nil, err
	}
	
	go func() {
		if err := cmd.Wait(); err != nil {
			log.WithField("error", err.Error()).Error("External compression command exited non-zero.")
		} else {
			log.Debug("External compression finished successfully.")
		} 
	}()
	
	return rdr, err
}

func (c Filter) DecompressFileInPlace(filePath string) error {	
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Decompression Command")
	
	cmd := exec.Command(c.Command, "-d", filePath)
	err := cmd.Run()
	if err != nil {
		log.WithFields(logFields).Warn("DeCompression command failed.")
	}
	
	return err
}

// Decompress the given file and return the stream
func (c Filter) Decompress(filePath string) (io.ReadCloser, error) {
	cmd := exec.Command(c.Command, "-d","-c", filePath)
	rdr, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("Failed to get stdout pipe.")
		return nil, err
	}
	
	if err := cmd.Start(); err != nil {
		log.Errorf("External decompression command error:", err.Error())
		return nil, err
	}
	
	go func() {
		if err := cmd.Wait(); err != nil {
			log.WithField("error", err.Error()).Error("External compression command exited non-zero.")
		} else {
			log.Debug("External compression finished successfully.")
		} 
	}()
	
	return rdr, err
}