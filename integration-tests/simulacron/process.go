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

type SimulacronProcess struct {
	httpPort   int
	startIp    string
	ctx        context.Context
	cancelFunc context.CancelFunc
	lock       *sync.Mutex
	started    bool
	cmd *exec.Cmd
	baseUrl string
}

var globalInstance = &atomic.Value{}
var globalSimulacronMutex = &sync.Mutex{}
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
}
var simulacronPath = os.Getenv("SIMULACRON_PATH")

const (
	defaultHttpPort = 8188
	defaultStartIp  = "127.0.0.101"
)

func NewSimulacronProcess(httpPort int, startIp string) *SimulacronProcess {
	ctx, cancel := context.WithCancel(context.Background())
	return &SimulacronProcess{
		httpPort:   httpPort,
		startIp:    startIp,
		ctx:        ctx,
		cancelFunc: cancel,
		lock:       &sync.Mutex{},
		started:    false,
		cmd: 		nil,
		baseUrl: 	"http://127.0.0.1:" + strconv.FormatInt(int64(httpPort), 10),
	}
}

func (process *SimulacronProcess) Cancel() {
	process.lock.Lock()
	defer process.lock.Unlock()
	process.cancelInternal()
}

func (process *SimulacronProcess) cancelInternal() {
	process.cancelFunc()
	ctx, cancel := context.WithCancel(context.Background())
	process.ctx = ctx
	process.cancelFunc = cancel
	process.started = false
	tempCmd := process.cmd
	process.cmd = nil

	channel := make(chan bool)

	go func() {
		if tempCmd.Process != nil {
			err := tempCmd.Process.Kill()
			if err != nil {
				log.Warn("failed to kill simulacron process:", err)
			}
			state, err := tempCmd.Process.Wait()
			if err != nil {
				log.Warn("failed to wait for simulacron process to exit:", err)
			}
			if state != nil && state.Exited() {
				log.Info("simulacron process exited with code:", state.ExitCode())
			}
			if state != nil && !state.Exited() {
				log.Warn("simulacron process did not exit")
			}
		} else {
			err := tempCmd.Wait()
			if err != nil {
				log.Warn("failed to wait for simulacron launch command to exit:", err)
			}
		}
		channel <- true
	}()

	select {
	case <-channel:
		return
	case <-time.After(60 * time.Second):
		log.Warn("Timeout while waiting for simulacron process to stop.")
		return
	}
}

func (process *SimulacronProcess) Start() error {

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

			if strings.Contains(line, "Created nodes will start with ip") ||
				strings.Contains(line, "Address already in use") {
				stopChannel <- nil
				return
			}

			if err != nil &&
				(err == io.EOF || strings.Contains(err.Error(), "file already closed")) {
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
				(err == io.EOF || strings.Contains(err.Error(), "file already closed")) {
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

	err := <- mainChannel
	if err == nil {
		process.started = true
		return nil
	} else {
		process.cancelInternal()
		return err
	}
}

func GetGlobalSimulacronProcess() (*SimulacronProcess, error) {
	instance := globalInstance.Load()
	if instance != nil {
		return instance.(*SimulacronProcess), nil
	}

	globalSimulacronMutex.Lock()
	defer globalSimulacronMutex.Unlock()

	instance = globalInstance.Load()
	if instance != nil {
		return instance.(*SimulacronProcess), nil
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
