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
	"sync"
	
	log "github.com/Sirupsen/logrus"
	//"github.com/davecgh/go-spew/spew"
)

// Implement a logrus-style writer for use with exec stanzas. Passing in a
// logrus entry then uses that entry for subsequent output.
type LogWriter struct {
	fnLog func(... interface{})
}

func (lw LogWriter) Write (p []byte) (n int, err error) {
	lw.fnLog(string(p))
	return len(p),nil
}

// Takes a function which will do the actual logging (should be a logrus
// log level function and returns a log writer which implements io.Writer
func NewLogWriter(fnLog func(... interface{}) ) *LogWriter {
	var lw LogWriter
	lw.fnLog = fnLog
	return &lw
}

var (
	mimeQueryCh chan string
	mimeResponseCh chan mimeResponse
)

type mimeResponse struct {
	mimetype string
	err error
}

func init() {
	// Start the magic mime worker
	mimeQueryCh = make(chan string,0)
	mimeResponseCh = make(chan mimeResponse,0)
	go magicMimeWorker()
}

// Interface of an external handler type for dealing with library compression
type ExternalHandler interface {
	// Stream compression/decompression from file
	Compress(filePath string) (CompressionProcess, error)
	Decompress(filePath string) (CompressionProcess, error)
	
	// Pure stream handlers
	CompressStream(io.Reader) (CompressionProcess, error)
	DecompressStream(io.ReadCloser) (CompressionProcess, error)
	
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

// Represents a spawned external compression process. Consists of a ReadCloser
// interfaced with an additional result field for retreiving the status code
// of the job.
type CompressionProcess interface {
	Result() int	// Get the result of the compressor. This function will block until the result is availble.

	Read(p []byte) (n int, err error)
	Close() error
}

// Implements the ReadCloser interface to allow safely shutting down remotely
// invoked Command pipes.
type CompressionJob struct {
	cmd *exec.Cmd
	pipe io.ReadCloser
	result int

	termFlag bool	// True if we deliberately killed this job via Close()

	// Used to make Result
	wg sync.WaitGroup
}

// Creates a new compression job and initializes the wait group
func newCompressionJob(cmd *exec.Cmd, pipe io.ReadCloser) *CompressionJob {
	job := CompressionJob{}
	job.cmd = cmd
	job.pipe = pipe
	job.wg.Add(1)

	return &job
}

func (rwc CompressionJob) Read(p []byte) (n int, err error) {
	return rwc.pipe.Read(p)
}

func (this *CompressionJob) Close() error {
	// If process not existed, request kill
	if this.cmd.ProcessState != nil {
		// Close requested, so kill the process, then close it's pipe.
//		err := this.cmd.Process.Signal(syscall.SIGINT)
//		if err != nil {
//			log.WithField("error", err.Error()).Error("Error sending signal to external process")
//		}

//		// If the int isn't respected after a few seconds, do a term.
//		t := time.NewTimer(time.Second * 3)
//		<- t.C
//
		log.Debug("Terminating still active compression command")
		err := this.cmd.Process.Signal(syscall.SIGTERM)
		if err != nil {
			log.WithField("error", err.Error()).Error("Error sending signal to external process")
		}
		this.termFlag = true
	}
	this.pipe.Close()
	return this.getResult()
}

func (this *CompressionJob) getResult() error {
	if err := this.cmd.Wait(); err != nil {
		// Result is forced to 0 (success) if we forcibly closed the pipe.
		if !this.termFlag {
			if exiterr, ok := err.(*exec.ExitError); ok {
				// The program has exited with an exit code != 0

				// This works on both Unix and Windows. Although package
				// syscall is generally platform dependent, WaitStatus is
				// defined for both Unix and Windows and in both cases has
				// an ExitStatus() method with the same signature.
				if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
					this.result = status.ExitStatus()
				}
			} else {
				log.Fatalf("cmd.Wait: %v", err)
			}
		}
	}

	this.wg.Done()	// Clear the waiting for results
	return nil
}

// Returns the exit status of the compression command. Blocks until the compression
// command is actually terminated.
func (this *CompressionJob) Result() int {
	if this.cmd.ProcessState == nil {
		this.getResult()
	}

	this.wg.Wait()	// Wait for command to exit
	return this.result
}

// Map mimetypes to stream compressors
var mimeMap map[string]string = map[string]string {
	"application/x-bzip2" : "bzip2",
	"bzip2" : "bzip2",

	"application/gzip" : "gzip",
	"application/x-gzip" : "gzip",
	"gzip" : "gzip",

	"application/x-xz" : "xz",
	"xz" : "xz",

	"text/plain" : "cat",
	"text" : "cat",
	"application/x-empty" : "cat",
	"inode/x-empty" : "cat",
}

// Map of stream compressors
var filtersMap map[string]Filter = map[string]Filter{
	"bzip2" : Filter{
		Command: "bzip2",
		CompressFlags: []string{"-c"},
		DecompressFlags: []string{"-d", "-c"},

		CompressStreamFlags: []string{"-c"},
		DecompressStreamFlags: []string{"-d", "-c"},
		
		CompressInPlaceFlags: []string{},
		DecompressInPlaceFlags: []string{"-d"},
	},
	"gzip" : Filter{
		Command: "gzip",
		CompressFlags: []string{"-c"},
		DecompressFlags: []string{"-d", "-c"},
	
		CompressStreamFlags: []string{"-c"},
		DecompressStreamFlags: []string{"-d", "-c"},
		
		CompressInPlaceFlags: []string{},
		DecompressInPlaceFlags: []string{"-d"},
	},
	"xz" : Filter{
		Command: "xz",
		CompressFlags: []string{"-c"},
		DecompressFlags: []string{"-d", "-c"},
	
		CompressStreamFlags: []string{"-c"},
		DecompressStreamFlags: []string{"-d", "-c"},
		
		CompressInPlaceFlags: []string{},
		DecompressInPlaceFlags: []string{"-d"},
	},
	"cat" : Filter{
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

// Go-routine which serves magicmime requests because libmagic is not thread
// safe.
func magicMimeWorker() {
	err:= magicmime.Open(magicmime.MAGIC_MIME_TYPE |
		magicmime.MAGIC_SYMLINK | magicmime.MAGIC_ERROR)
	if err != nil {
		log.Fatalln("libmagic initialization failure", err.Error())
	}
	defer magicmime.Close()

	// Listen
	for filePath := range mimeQueryCh {
		mimetype, err := magicmime.TypeByFile(filePath)
		mimeResponseCh <- mimeResponse{mimetype, err}
	}
}

// Do a filemagic lookup and return a handler interface for the given type
func GetFileTypeExternalHandler(filePath string) (ExternalHandler, error) {
    mimeQueryCh <- filePath
	r := <- mimeResponseCh
	if r.err != nil {
		return nil, r.err
	}
    return GetExternalHandlerFromMimeType(r.mimetype)
}

func GetExternalHandlerFromMimeType(mimeType string) (ExternalHandler, error) {
	handlername, ok := mimeMap[mimeType]
    if !ok {
    	// Try splitting on the / and looking for a bulk handler
    	firstpart := strings.Split(mimeType, "/")[0]
    	handlername, ok = mimeMap[firstpart]
    	if !ok {
    		return nil, error(UnknownFileType{mimeType})
    	}
    }

	handler := filtersMap[handlername]
    
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

func (c Filter) Compress(filePath string) (CompressionProcess, error) {
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

	return newCompressionJob(cmd, rdr), err
}

func (c Filter) CompressStream(rd io.Reader) (CompressionProcess, error) {
	var logFields = log.Fields{"compressCmd" : c.Command }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command,c.CompressStreamFlags...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Don't pass on parent signals

	cmd.Stdin = rd
	cmd.Stderr = NewLogWriter(log.WithField("extcompress", "CompressStream").Debug)
	
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

	return newCompressionJob(cmd, rdr), err
}

// Call the compression utility in standalone compression mode
func (c Filter) CompressFileInPlace(filePath string) error {	
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command, append(c.CompressInPlaceFlags, filePath)...)

	cmd.Stderr = NewLogWriter(log.WithField("extcompress", "CompressFileInPlace").Debug)

	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Don't pass on parent signals
	err := cmd.Run()
	if err != nil {
		log.WithFields(logFields).WithField("error", err.Error()).Warn("Compression command failed.")
	}
	
	return err
}

func (c Filter) DecompressStream(rd io.ReadCloser) (CompressionProcess, error) {
	var logFields = log.Fields{"compressCmd" : c.Command }
	log.WithFields(logFields).Info("External Compression Command")
	
	cmd := exec.Command(c.Command,c.DecompressStreamFlags...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Don't pass on parent signals
	cmd.Stdin = rd
	cmd.Stderr = NewLogWriter(log.WithField("extcompress", "DecompressStream").Debug)

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

	return newCompressionJob(cmd, rdr), err
}

func (c Filter) DecompressFileInPlace(filePath string) error {	
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Decompression Command")
	
	cmd := exec.Command(c.Command, append(c.DecompressInPlaceFlags, filePath)...)

	cmd.Stderr = NewLogWriter(log.WithField("extcompress", "DecompressFileInPlace").Debug)

	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Don't pass on parent signals
	err := cmd.Run()
	if err != nil {
		log.WithFields(logFields).Warn("DeCompression command failed.")
	}
	
	return err
}

// Decompress the given file and return the stream
func (c Filter) Decompress(filePath string) (CompressionProcess, error) {
	var logFields = log.Fields{"compressCmd" : c.Command, "filepath" : filePath }
	log.WithFields(logFields).Info("External Decompression Command")
	
	cmd := exec.Command(c.Command, append(c.DecompressFlags, filePath)...)

	cmd.Stderr = NewLogWriter(log.WithField("extcompress", "Decompress").Debug)

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
	
	return newCompressionJob(cmd, rdr), err
}
