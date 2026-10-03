//go:build windows

package acpserver

import (
	"errors"
	"os"
	"os/exec"
	"sync"

	"golang.org/x/sys/windows"
)

var mcpJobs sync.Map

func executableOwnedByCurrentUser(os.FileInfo) bool { return true }

func prepareMCPCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &windows.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
}

func attachMCPProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return errors.New("MCP process did not start")
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return err
	}
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err != nil {
		windows.CloseHandle(job)
		return err
	}
	defer windows.CloseHandle(process)
	if err := windows.AssignProcessToJobObject(job, process); err != nil {
		windows.CloseHandle(job)
		return err
	}
	mcpJobs.Store(cmd.Process.Pid, job)
	return nil
}

func killMCPProcessTree(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	value, ok := mcpJobs.LoadAndDelete(cmd.Process.Pid)
	if !ok {
		return cmd.Process.Kill()
	}
	job := value.(windows.Handle)
	err := windows.TerminateJobObject(job, 1)
	closeErr := windows.CloseHandle(job)
	return errors.Join(err, closeErr)
}
