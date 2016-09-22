package cpcp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/jessevdk/go-flags"
)

func CPCP(args []string) error {
	cfg, args, err := parseArgs(args)
	if err != nil {
		return err
	}
	if cfg.verbosity > 1 {
		fmt.Printf("cfg is %#v\n", cfg)
		fmt.Printf("args are %#v\n", args)
	}
	switch len(args) {
	case 0:
		return errors.New("nothing specified to copy")
	case 1:
		return fmt.Errorf("missing destination parameter after %q", args[0])
	}
	srcs := args[:len(args)-1]
	dst := args[len(args)-1]

	u := syscall.Umask(0)
	syscall.Umask(u)
	cfg.umask = os.FileMode(u & 0x1ff)

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

	if len(srcs) == 1 {
		src := srcs[0]
		dstfi, err := os.Stat(dst)
		if err == nil && dstfi.IsDir() {
			dst = path.Join(dst, path.Base(src))
		}
		var srcfi os.FileInfo
		if cfg.noDereference {
			srcfi, err = os.Lstat(src)
		} else {
			srcfi, err = os.Stat(src)
		}
		if err != nil {
			errs <- fmtErr(src, err)
		} else if srcfi.IsDir() && !cfg.recursive {
			errs <- fmt.Sprintf("omitting directory %q", src)
		} else {
			ct := <-freeCopyTasks
			ct.src = src
			ct.dst = dst
			ct.srcfi = srcfi
			wg.Add(1)
			copyTasks <- ct
		}
	} else {
		dstfi, err := os.Stat(dst)
		if err != nil && !os.IsNotExist(err) {
			errs <- fmtErr(dst, err)
		} else if os.IsNotExist(err) || !dstfi.IsDir() {
			errs <- fmt.Sprintf("target %q is not a directory", dst)
		} else {
			for _, src := range srcs {
				var srcfi os.FileInfo
				if cfg.noDereference {
					srcfi, err = os.Lstat(src)
				} else {
					srcfi, err = os.Stat(src)
				}
				if err != nil {
					errs <- fmtErr(src, err)
					continue
				}
				if srcfi.IsDir() && !cfg.recursive {
					errs <- fmt.Sprintf("omitting directory %q", src)
					continue
				}
				ct := <-freeCopyTasks
				ct.src = src
				ct.dst = path.Join(dst, path.Base(src))
				ct.srcfi = srcfi
				wg.Add(1)
				copyTasks <- ct
			}
		}
	}

	wg.Wait()

	close(msgs)
	<-msgsDone
	close(errs)
	<-errsDone
	finalErrCount := atomic.LoadUint32(&errCount)
	if finalErrCount > 0 {
		return fmt.Errorf("there were %d errors", finalErrCount)
	}
	return nil
}

type config struct {
	verbosity     int
	noDereference bool
	recursive     bool
	messageBuffer int
	errBuffer     int
	parallelTasks int
	readdirBuffer int
	copyBuffer    int
	umask         os.FileMode
	preserveLinks bool
	preserveMode  bool
}

type parseOpts struct {
	Verbosity     []bool `short:"v" long:"verbose" description:"Outputs details of work done."`
	Dereference   bool   `short:"L" long:"dereference" description:"Follow links in source and copy what is pointed to."`
	NoDereference bool   `short:"P" long:"no-dereference" description:"Does not follow links in source and instead makes similar links in destination."`
	Preserve      string `long:"preserve" description:"Preserve specified attributes. Available: links,all"`
	Recursive     bool   `short:"r" long:"recursive" description:"Copies directories recursively."`
	Recursive2    bool   `short:"R" description:"Alias for --recursive"`
	ShortcutA     bool   `short:"a" long:"archive" description:"Same as -dR --preserve=all"`
	ShortcutD     bool   `short:"d" description:"Same as --no-dereference --preserve=links"`
}

func parseArgs(args []string) (*config, []string, error) {
	cfg := &config{}
	setPreserve := func(arg string) error {
		preserves := strings.Split(arg, ",")
		for _, preserve := range preserves {
			switch preserve {
			case "":
			case "links":
				cfg.preserveLinks = true
			case "mode":
				cfg.preserveMode = true
			case "all":
				cfg.preserveLinks = true
				cfg.preserveMode = true
			default:
				return fmt.Errorf("unsupported preserve specification %q\n", preserve)
			}
		}
		return nil
	}
	oargs := args
L:
	for {
		for i, arg := range oargs {
			if strings.HasPrefix(arg, "--preserve=") {
				if err := setPreserve(arg[len("--preserve="):]); err != nil {
					return nil, nil, err
				}
				oargs = append(oargs[:i], oargs[i+1:]...)
				goto L
			}
			switch arg {
			case "-a", "--archive":
				ooargs := make([]string, len(oargs))
				copy(ooargs, oargs)
				// TODO: Should be -dR and not -d -R which are the same thing,
				// but not parsed the same right now. Going to have to rework
				// things based on this. Probably will have to drop using
				// github.com/jessevdk/go-flags and do it ourselves because the
				// original cp is too complex.
				oargs = append(oargs[:i], "-d", "-R", "--preserve=all")
				oargs = append(oargs, ooargs[i+1:]...)
				goto L
			case "-d":
				ooargs := make([]string, len(oargs))
				copy(ooargs, oargs)
				oargs = append(oargs[:i], "--no-dereference", "--preserve=links")
				oargs = append(oargs, ooargs[i+1:]...)
				goto L
			case "--preserve":
				if err := setPreserve(oargs[i+1]); err != nil {
					return nil, nil, err
				}
				oargs = append(oargs[:i], oargs[i+2:]...)
				goto L
			case "-R":
				oargs[i] = "-r"
			}
		}
		break
	}
	opts := &parseOpts{}
	args, err := flags.ParseArgs(opts, oargs)
	if err != nil {
		return nil, nil, err
	}
	noderef := false
	// cp allows you specify these conflicting options and seems to just
	// use the last one specified.
	// NOTE: Will need to keep on top of when we add new options that
	// implicitly set these options as well.
	for _, arg := range oargs {
		switch arg {
		case "-L", "--dereference":
			noderef = false
		case "-P", "--no-dereference":
			noderef = true
		}
	}
	cfg.verbosity = len(opts.Verbosity)
	cfg.noDereference = noderef
	cfg.recursive = opts.Recursive
	cfg.messageBuffer = 1000
	cfg.errBuffer = 1000
	cfg.parallelTasks = 1000
	cfg.readdirBuffer = 1000
	cfg.copyBuffer = 65536
	return cfg, args, nil
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
			if !cfg.recursive {
				errs <- fmt.Sprintf("omitting directory %q", src)
				continue
			}
			m := srcfi.Mode()
			if !cfg.preserveMode {
				m &= cfg.umask
			}
			if err := os.Mkdir(dst, m); err != nil {
				if !os.IsExist(err) {
					errs <- fmtErr(dst, err)
				}
			}
			// The above Mkdir doesn't always seem to apply the exact mode we
			// asked it to.
			if cfg.preserveMode {
				if err := os.Chmod(dst, m); err != nil {
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
					m := srcfi.Mode()
					if !cfg.preserveMode {
						m &= cfg.umask
					}
					if err := os.Chmod(dst, m); err != nil {
						errs <- fmtErr(dst, err)
					}
				}
			}
		} else if srcfi.Mode()|os.ModeSymlink != 0 {
			target, err := os.Readlink(src)
			if err != nil {
				errs <- fmtErr(src, err)
			} else if err = os.Symlink(target, dst); err != nil {
				errs <- fmtErr(dst, err)
			}
		}
		wg.Done()
	}
}
