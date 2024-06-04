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

func execCcm(arg ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel() // The cancel should be deferred so resources are cleaned up

	log.Infof("Executing ccm command: ccm %s", arg)
	// Create the command with our context
	var cmd *exec.Cmd

	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd.exe", append([]string{"/S", "/C", "ccm"}, arg...)...)
	} else {
		cmd = exec.CommandContext(ctx, "/usr/local/bin/ccm", arg...)
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
		return execCcm("create", name, "-v", version, "--dse")
	} else {
		return execCcm("create", name, "-v", version)
	}

}

func Add(seed bool, address string, remoteDebugPort int, jmxPort int, name string, isDse bool) (string, error) {
	var addArgs = []string{
		"-i", address, "-r", fmt.Sprintf("%d", remoteDebugPort), "-j", fmt.Sprintf("%d", jmxPort), name}
	if isDse {
		addArgs = append(addArgs, "--dse")
	}
	if seed {
		return execCcm(append([]string{"add", "-s"}, addArgs...)...)
	} else {
		return execCcm(append([]string{"add"}, addArgs...)...)
	}
}

func StopCurrent() (string, error) {
	return execCcm("stop")
}

func Remove(name string) (string, error) {
	return execCcm("remove", name)
}

func Switch(name string) (string, error) {
	return execCcm("switch", name)
}

func UpdateConf(yamlChanges ...string) (string, error) {
	return execCcm(append([]string{"updateconf"}, yamlChanges...)...)
}

func Start(jvmArgs ...string) (string, error) {
	newJvmArgs := make([]string, len(jvmArgs)*2)
	for i := 0; i < len(newJvmArgs); i += 2 {
		newJvmArgs[i] = "--jvm_arg"
		newJvmArgs[i+1] = jvmArgs[i]
	}

	if runtime.GOOS == "windows" {
		return execCcm(append([]string{"start", "--quiet-windows", "--wait-for-binary-proto"}, newJvmArgs...)...)
	} else {
		return execCcm(append([]string{"start", "--verbose", "--root", "--wait-for-binary-proto"}, newJvmArgs...)...)
	}
}

func StartNode(nodeName string, jvmArgs ...string) (string, error) {
	newJvmArgs := make([]string, len(jvmArgs)*2)
	for i := 0; i < len(newJvmArgs); i += 2 {
		newJvmArgs[i] = "--jvm_arg"
		newJvmArgs[i+1] = jvmArgs[i]
	}

	if runtime.GOOS == "windows" {
		return execCcm(append([]string{nodeName, "start", "--quiet-windows", "--wait-for-binary-proto"}, newJvmArgs...)...)
	} else {
		return execCcm(append([]string{nodeName, "start", "--verbose", "--root", "--wait-for-binary-proto"}, newJvmArgs...)...)
	}
}

func Stop() (string, error) {
	return execCcm(append([]string{"stop"})...)
}

func StopNode(nodeName string) (string, error) {
	return execCcm(nodeName, "stop")
}

func RemoveNode(nodeName string) (string, error) {
	return execCcm(nodeName, "remove")
}
