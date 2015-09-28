/*
	package which provides a set of helpers to wrap external compression
	commands behind writer/reader interfaces.
	
	This whole library would benefit from a decent shlex-er type interface to
	make specifying the filters less verbose.
*/

package extcompress

import (
	"syscall"
	"os/exec"
	"io"
	"strings"
	"github.com/rakyll/magicmime"
	
	log "github.com/Sirupsen/logrus"
)

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
	CommandStreamCompress() string
	CommandStreamDecompress() string
	MimeType() string
}

// Handles most unix-style filter commands and implements the externalhandler
// interface. The filename, where necessary, is appended to the flags.
type Filter struct {
	Command string
	
	CompressFlags []string
	DecompressFlags []string
	
	CompressStreamFlags []string
	DecompressStreamFlags []string
	
	CompressInPlaceFlags []string
	DecompressInPlaceFlags []string
	
	mimeType string
}

// Implements the ReadCloser interface to allow safely shutting down remotely
// invoked Command pipes.
type ReadWaitCloser struct {
	cmd *exec.Cmd
	pipe io.ReadCloser
}

func (rwc ReadWaitCloser) Read(p []byte) (n int, err error) {
	return rwc.pipe.Read(p)
}

func (rwc ReadWaitCloser) Close() error {
	// Close requested, so ask the process to die, then close it's pipe.
	err := rwc.cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		log.WithField("error", err.Error()).Error("Error sending signal to external process")
	}

	err = rwc.cmd.Wait()
	if err != nil {
		log.WithField("error", err.Error()).Error("External compression command exited non-zero.")
		rwc.pipe.Close()
		return err
	} else {
		log.Debug("External compression finished successfully.")
	} 
	
	err = rwc.pipe.Close()
	return err
}

// Map of stream compressors
var filtersMap map[string]Filter = map[string]Filter{
	"application/x-bzip2" : Filter{ 
		Command: "bzip2",
		CompressFlags: []string{"-c"},
		DecompressFlags: []string{"-d", "-c"},
	
		CompressStreamFlags: []string{"-c"},
		DecompressStreamFlags: []string{"-d", "-c"},
		
		CompressInPlaceFlags: []string{},
		DecompressInPlaceFlags: []string{"-d"},
	},
	"application/gzip" : Filter{ 
		Command: "gzip",
		CompressFlags: []string{"-c"},
		DecompressFlags: []string{"-d", "-c"},
	
		CompressStreamFlags: []string{"-c"},
		DecompressStreamFlags: []string{"-d", "-c"},
		
		CompressInPlaceFlags: []string{},
		DecompressInPlaceFlags: []string{"-d"},
	},
	"application/x-xz" : Filter{ 
		Command: "xz",
		CompressFlags: []string{"-c"},
		DecompressFlags: []string{"-d", "-c"},
	
		CompressStreamFlags: []string{"-c"},
		DecompressStreamFlags: []string{"-d", "-c"},
		
		CompressInPlaceFlags: []string{},
		DecompressInPlaceFlags: []string{"-d"},
	},
	"text" : Filter{ 
		Command: "cat",
		CompressFlags: []string{},
		DecompressFlags: []string{},
	
		CompressStreamFlags: []string{},
		DecompressStreamFlags: []string{},
		
		CompressInPlaceFlags: []string{},
		DecompressInPlaceFlags: []string{},
	},
	"application/x-empty" : Filter{ 
		Command: "cat",
		CompressFlags: []string{},
		DecompressFlags: []string{},
	
		CompressStreamFlags: []string{},
		DecompressStreamFlags: []string{},
		
		CompressInPlaceFlags: []string{},
		DecompressInPlaceFlags: []string{},
	},
	"inode/x-empty" : Filter{ 
		Command: "cat",
		CompressFlags: []string{},
		DecompressFlags: []string{},
	
		CompressStreamFlags: []string{},
		DecompressStreamFlags: []string{},
		
		CompressInPlaceFlags: []string{},
		DecompressInPlaceFlags: []string{},
	},
}

// Check that all handlers are properly registered, fail hard if they're not.
func CheckHandlers() {
	for k, v := range filtersMap {
		hlog := log.WithField("mimetype", k).WithField("handler", v)
		_, err := exec.LookPath(v.Command)
		if err != nil {
			hlog.Fatal("Handler unavailable!")
		}
	}
}

// Do a filemagic lookup and return a handler interface for the given type
func GetFileTypeExternalHandler(filePath string) (ExternalHandler, error) {
	err:= magicmime.Open(magicmime.MAGIC_MIME_TYPE |
		magicmime.MAGIC_SYMLINK | magicmime.MAGIC_ERROR)
    if err != nil {
        return nil, err
    }
	defer magicmime.Close()

    mimetype, err := magicmime.TypeByFile(filePath)
    if err != nil {
        return nil, err
    }
    
    return GetExternalHandlerFromMimeType(mimetype)
}

func GetExternalHandlerFromMimeType(mimeType string) (ExternalHandler, error) {
	handler, ok := filtersMap[mimeType]
    if !ok {
    	// Try splitting on the / and looking for a bulk handler
    	firstpart := strings.Split(mimeType, "/")[0]
    	handler, ok = filtersMap[firstpart]
    	if !ok {
    		return nil, error(UnknownFileType{"mimeType"})
    	}
    }
    
    handler.mimeType = mimeType
    extHandler := ExternalHandler(handler)
    return extHandler, nil
}

type UnknownFileType struct {
	MimeType string	
}
func (r UnknownFileType) Error() string {
	return "This file type is not known to us."
}

func (c Filter) MimeType() string {
	return c.mimeType
}

func (c Filter) CommandStreamCompress() string {
	return strings.Join(append([]string{c.Command}, c.CompressStreamFlags...), " ")
}

func (c Filter) CommandStreamDecompress() string {
	return strings.Join(append([]string{c.Command}, c.DecompressStreamFlags...), " ")
}

func (c Filter) Compress(filePath string) (io.ReadCloser, error) {
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command, append(c.CompressFlags, filePath)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Don't pass on parent signals

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
	
	return io.ReadCloser(ReadWaitCloser{cmd, rdr}), err
}

func (c Filter) CompressStream(rd io.ReadCloser) (io.ReadCloser, error) {
	var logFields = log.Fields{"compressCmd" : c.Command }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command,c.CompressStreamFlags...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Don't pass on parent signals

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
	
	return io.ReadCloser(ReadWaitCloser{cmd, rdr}), err
}

// Call the compression utility in standalone compression mode
func (c Filter) CompressFileInPlace(filePath string) error {	
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command, append(c.CompressInPlaceFlags, filePath)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Don't pass on parent signals
	err := cmd.Run()
	if err != nil {
		log.WithFields(logFields).Warn("Compression command failed.")
	}
	
	return err
}

func (c Filter) DecompressStream(rd io.ReadCloser) (io.ReadCloser, error) {
	var logFields = log.Fields{"compressCmd" : c.Command }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command,c.DecompressStreamFlags...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Don't pass on parent signals
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
	
	return io.ReadCloser(ReadWaitCloser{cmd, rdr}), err
}

func (c Filter) DecompressFileInPlace(filePath string) error {	
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Decompression Command")
	
	cmd := exec.Command(c.Command, append(c.DecompressInPlaceFlags, filePath)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Don't pass on parent signals
	err := cmd.Run()
	if err != nil {
		log.WithFields(logFields).Warn("DeCompression command failed.")
	}
	
	return err
}

// Decompress the given file and return the stream
func (c Filter) Decompress(filePath string) (io.ReadCloser, error) {
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Decompression Command")
	
	cmd := exec.Command(c.Command, append(c.DecompressFlags, filePath)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Don't pass on parent signals
	rdr, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("Failed to get stdout pipe.")
		return nil, err
	}
	
	if err := cmd.Start(); err != nil {
		log.Errorf("External decompression command error:", err.Error())
		return nil, err
	}
	
	return io.ReadCloser(ReadWaitCloser{cmd, rdr}), err
}