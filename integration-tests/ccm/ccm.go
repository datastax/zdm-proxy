package ccm

import (
	"context"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

const cmdTimeout = 5 * time.Minute

func execCcm(arg string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel() // The cancel should be deferred so resources are cleaned up

	log.Infof("Executing ccm command: ccm %s", arg)
	// Create the command with our context
	var cmd *exec.Cmd

	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd.exe", "/c ccm " + arg)
	} else {
		cmd = exec.CommandContext(ctx, "/usr/local/bin/ccm", arg)
	}

	out, err := cmd.CombinedOutput()

	// We want to check the context error to see if the timeout was executed.
	// The error returned by cmd.Output() will be OS specific based on what
	// happens when a process is killed.
	if ctx.Err() == context.DeadlineExceeded {
		return "", errors.New("command timed out")
	}

	// If there's no context error, we know the command completed (or errored).
	var output = string(out)
	if len(strings.TrimSpace(output)) > 0 {
		log.Info("CCM Output:", output)
	}
	if err != nil {
		return output, errors.New(err.Error() + ". Output: " + output)
	}

	return output, nil
}

func Create(name string, version string, isDse bool) (string, error) {
	if isDse {
		return execCcm(fmt.Sprintf("create %s -v %s --dse", name, version))
	} else {
		return execCcm(fmt.Sprintf("create %s -v %s", name, version))
	}

}

func Add(seed bool, address string, remoteDebugPort int, jmxPort int, name string) (string, error) {
	var addArgs = fmt.Sprintf("-i %s -r %d -j %d %s", address, remoteDebugPort, jmxPort, name)
	if seed {
		return execCcm(fmt.Sprintf("add -s %s", addArgs))
	} else {
		return execCcm(fmt.Sprintf("add %s", addArgs))
	}
}

func RemoveCurrent() (string, error) {
	return execCcm("remove")
}

func Remove(name string) (string, error) {
	return execCcm(fmt.Sprintf("remove %s", name))
}

func Switch(name string) (string, error) {
	return execCcm(fmt.Sprintf("switch %s", name))
}

func Start() (string, error) {
	if runtime.GOOS == "windows" {
		return execCcm("start --quiet-windows --wait-for-binary-proto")
	} else {
		return execCcm("start --wait-for-binary-proto")
	}
}