package simulacron

import (
	"bufio"
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Process struct {
	httpPort   int
	startIp    string
	ctx        context.Context
	cancelFunc context.CancelFunc
	lock       *sync.Mutex
	started    bool
	cmd        *exec.Cmd
	baseUrl    string
	waitGroup  *sync.WaitGroup
	failedBind bool
}

var globalInstance = &atomic.Value{}
var globalSimulacronMutex = &sync.Mutex{}
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
}
var simulacronPath = os.Getenv("SIMULACRON_PATH")

const (
	defaultHttpPort = 8188
	defaultStartIp  = "127.0.0.40"
)

func NewSimulacronProcess(httpPort int, startIp string) *Process {
	ctx, cancel := context.WithCancel(context.Background())
	return &Process{
		httpPort:   httpPort,
		startIp:    startIp,
		ctx:        ctx,
		cancelFunc: cancel,
		lock:       &sync.Mutex{},
		started:    false,
		cmd:        nil,
		baseUrl:    "http://127.0.0.1:" + strconv.FormatInt(int64(httpPort), 10),
		waitGroup:  &sync.WaitGroup{},
		failedBind: false,
	}
}

func (process *Process) Cancel() {
	process.lock.Lock()
	defer process.lock.Unlock()
	process.cancelInternal()
}

func (process *Process) cancelInternal() {
	process.cancelFunc()
	ctx, cancel := context.WithCancel(context.Background())
	process.ctx = ctx
	process.cancelFunc = cancel
	process.started = false
	process.cmd = nil

	channel := make(chan bool)

	go func() {
		process.waitGroup.Wait()
		channel <- true
	}()

	select {
	case <-channel:
		if process.failedBind {
			log.Info("Simulacron process did not launch because another instance was already running, " +
				"skipping process clean up.")
			return
		} else {
			log.Infof("Simulacron process exited. Check previous log messages for the exit code.")
			return
		}
	case <-time.After(60 * time.Second):
		log.Warn("Timeout while waiting for simulacron process to stop.")
		return
	}
}

func (process *Process) Start() error {

	if process.started {
		return nil
	}

	process.lock.Lock()
	defer process.lock.Unlock()

	if process.started {
		return nil
	}

	process.cmd = exec.CommandContext(
		process.ctx,
		"java",
		"-jar",
		simulacronPath,
		"--ip",
		process.startIp,
		"-p",
		strconv.FormatInt(int64(process.httpPort), 10))

	process.cmd.Dir = filepath.Dir(simulacronPath)

	outPipe, errOut := process.cmd.StdoutPipe()
	outReader := bufio.NewReader(outPipe)
	errPipe, errErr := process.cmd.StderrPipe()
	errReader := bufio.NewReader(errPipe)

	startErr := process.cmd.Start()

	if errOut != nil {
		log.Error("Simulacron stdout error", startErr.Error())
		return errOut
	}

	if errErr != nil {
		log.Error("Simulacron stderr error", startErr.Error())
		outPipe.Close()
		return errErr
	}

	if startErr != nil {
		log.Error("Simulacron start error", startErr.Error())
		outPipe.Close()
		errPipe.Close()
		return startErr
	}

	tempCmd := process.cmd
	process.waitGroup.Add(1)
	go func() {
		defer process.waitGroup.Done()
		state, err := tempCmd.Process.Wait()
		if process.failedBind {
			return
		}
		if err != nil {
			log.Errorf("failed to wait for simulacron process to exit: %s", err)
			return
		}
		if state.Exited() {
			log.Infof("Simulacron process exited with code: %d", state.ExitCode())
		} else {
			log.Infof("Simulacron process was killed: %s", state.String())
		}
	}()

	stopChannel := make(chan error, 10)
	mainChannel := make(chan error, 10)

	go func() {
		select {
		case err := <-stopChannel:
			mainChannel <- err
		case <-time.After(60 * time.Second):
			mainChannel <- errors.New("timeout while waiting for simulacron to be ready")
		}
		errPipe.Close()
		outPipe.Close()
	}()

	go func() {
		for {
			line, err := outReader.ReadString('\n')

			if len(strings.TrimSpace(line)) > 0 {
				log.Info("Simulacron StdOut:", line)
			}

			if strings.Contains(line, "Created nodes will start with ip") {
				stopChannel <- nil
				return
			}

			if strings.Contains(line, "Address already in use") {
				process.failedBind = true
				stopChannel <- nil
				return
			}

			if err != nil &&
				(errors.Is(err, io.EOF) || strings.Contains(err.Error(), "file already closed")) {
				stopChannel <- err
				return
			}

			if err != nil {
				log.Error("Simulacron error:", err.Error())
				stopChannel <- err
				return
			}
		}
	}()

	go func() {
		for {
			line, err := errReader.ReadString('\n')

			if len(strings.TrimSpace(line)) > 0 {
				log.Info("Simulacron StdErr:", line)
			}

			if err != nil &&
				(errors.Is(err, io.EOF) || strings.Contains(err.Error(), "file already closed")) {
				stopChannel <- err
				return
			}

			if err != nil {
				log.Error("Simulacron error:", err.Error())
				stopChannel <- err
				return
			}
		}
	}()

	err := <-mainChannel
	if err == nil {
		process.started = true
		return nil
	} else {
		process.cancelInternal()
		return err
	}
}

func GetGlobalSimulacronProcess() *Process {
	instance := globalInstance.Load()
	if instance != nil {
		return instance.(*Process)
	}

	return nil
}

func GetOrCreateGlobalSimulacronProcess() (*Process, error) {
	instance := globalInstance.Load()
	if instance != nil {
		return instance.(*Process), nil
	}

	globalSimulacronMutex.Lock()
	defer globalSimulacronMutex.Unlock()

	instance = globalInstance.Load()
	if instance != nil {
		return instance.(*Process), nil
	}

	newInstance := NewSimulacronProcess(defaultHttpPort, defaultStartIp)
	err := newInstance.Start()

	if err != nil {
		return nil, err
	} else {
		globalInstance.Store(newInstance)
		return newInstance, nil
	}
}
