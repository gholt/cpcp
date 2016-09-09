package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
)

type config struct {
	verbosity     int
	messageBuffer int
	errBuffer     int
	parallelTasks int
	readdirBuffer int
	copyBuffer    int
}

type copyTask struct {
	src   string
	dst   string
	srcfi os.FileInfo
}

func fmtErr(pth string, err error) string {
	rv := err.Error()
	if rv == "" {
		rv = "unknown error"
	}
	if pth != "" {
		rv = pth + ": " + rv
	}
	_, filename, line, ok := runtime.Caller(1)
	if ok {
		rv = fmt.Sprintf("%s @%s:%d", rv, path.Base(filename), line)
	}
	return rv
}

func main() {
	cfg := &config{
		verbosity:     0,
		messageBuffer: 1000,
		errBuffer:     1000,
		parallelTasks: 1000,
		readdirBuffer: 1000,
		copyBuffer:    65536,
	}
	src := "/lib"
	dst := "/mnt/ea/libcopy3"

	msgs := make(chan string, cfg.messageBuffer)
	msgsDone := make(chan struct{})
	go func() {
		for {
			msg := <-msgs
			if msg == "" {
				break
			}
			fmt.Println(msg)
		}
		close(msgsDone)
	}()

	var errCount uint32
	errs := make(chan string, cfg.errBuffer)
	errsDone := make(chan struct{})
	go func() {
		for {
			err := <-errs
			if err == "" {
				break
			}
			atomic.AddUint32(&errCount, 1)
			fmt.Println(err)
		}
		close(errsDone)
	}()

	wg := &sync.WaitGroup{}

	copyTasks := make(chan *copyTask, cfg.parallelTasks)
	freeCopyTasks := make(chan *copyTask, cfg.parallelTasks)
	for i := 0; i < cfg.parallelTasks; i++ {
		freeCopyTasks <- &copyTask{}
		go copier(cfg, msgs, errs, wg, copyTasks, freeCopyTasks)
	}

	if srcfi, err := os.Lstat(src); err != nil {
		errs <- fmtErr(src, err)
	} else {
		if srcfi.IsDir() {
			dstfi, err := os.Lstat(dst)
			if err == nil && !dstfi.IsDir() {
				errs <- fmtErr(dst, errors.New("if source is a directory, destination must be as well"))
			} else {
				ct := <-freeCopyTasks
				ct.src = src
				ct.dst = dst
				ct.srcfi = srcfi
				wg.Add(1)
				copyTasks <- ct
			}
		} // TODO: handle files
	}
	wg.Wait()

	close(msgs)
	<-msgsDone
	close(errs)
	<-errsDone
	finalErrCount := atomic.LoadUint32(&errCount)
	if finalErrCount > 0 {
		fmt.Printf("There were %d errors.\n", finalErrCount)
		os.Exit(1)
	}
}

func copier(cfg *config, msgs chan string, errs chan string, wg *sync.WaitGroup, copyTasks chan *copyTask, freeCopyTasks chan *copyTask) {
	var localTasks []*copyTask
	copyBuf := make([]byte, cfg.copyBuffer)
	for {
		var src string
		var dst string
		var srcfi os.FileInfo
		if i := len(localTasks); i > 0 {
			i--
			ct := localTasks[i]
			localTasks = localTasks[:i]
			select {
			case fct := <-freeCopyTasks:
				fct.src = ct.src
				fct.dst = ct.dst
				fct.srcfi = ct.srcfi
				copyTasks <- fct
				continue
			default:
				src = ct.src
				dst = ct.dst
				srcfi = ct.srcfi
			}
		} else {
			ct := <-copyTasks
			src = ct.src
			dst = ct.dst
			srcfi = ct.srcfi
			freeCopyTasks <- ct
		}
		if cfg.verbosity > 0 {
			msgs <- fmt.Sprintf("%s -> %s", src, dst)
		}
		if srcfi.IsDir() {
			if err := os.Mkdir(dst, srcfi.Mode()); err != nil {
				if !os.IsExist(err) {
					errs <- fmtErr(dst, err)
				}
			}
			f, err := os.Open(src)
			if err != nil {
				errs <- fmtErr(src, err)
				wg.Done()
				continue
			}
			for {
				fis, err := f.Readdir(cfg.readdirBuffer)
				for _, fi := range fis {
					subsrc := path.Join(src, fi.Name())
					subdst := path.Join(dst, fi.Name())
					wg.Add(1)
					select {
					case ct := <-freeCopyTasks:
						ct.src = subsrc
						ct.dst = subdst
						ct.srcfi = fi
						copyTasks <- ct
					default:
						localTasks = append(localTasks, &copyTask{
							src:   subsrc,
							dst:   subdst,
							srcfi: fi,
						})
					}
				}
				if err != nil {
					f.Close()
					if err != io.EOF {
						errs <- fmtErr(src, err)
					}
					break
				}
			}
		} else if srcfi.Mode().IsRegular() {
			srcf, err := os.Open(src)
			if err != nil {
				errs <- fmtErr(src, err)
			} else {
				dstf, err := os.Create(dst)
				if err != nil {
					errs <- fmtErr(dst, err)
				} else {
					_, err := io.CopyBuffer(dstf, srcf, copyBuf)
					if err != nil {
						errs <- fmtErr(dst, err)
					}
					srcf.Close()
					dstf.Close()
					if err := os.Chmod(dst, srcfi.Mode()); err != nil {
						errs <- fmtErr(dst, err)
					}
				}
			}
		}
		wg.Done()
	}
}
