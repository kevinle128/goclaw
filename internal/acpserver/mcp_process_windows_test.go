//go:build windows

package acpserver

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

const windowsStillActive = 259

func TestACPWindowsJobObjectCleanup(t *testing.T) {
	if os.Getenv("GOCLAW_ACP_JOB_HELPER") == "1" {
		child := exec.Command("ping", "-t", "127.0.0.1")
		if err := child.Start(); err != nil {
			os.Exit(2)
		}
		fmt.Println(child.Process.Pid)
		select {}
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestACPWindowsJobObjectCleanup$")
	cmd.Env = append(os.Environ(), "GOCLAW_ACP_JOB_HELPER=1")
	prepareMCPCommand(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := attachMCPProcess(cmd); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 32)
	n, err := stdout.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(strings.TrimSpace(string(buffer[:n])))
	if err != nil {
		t.Fatal(err)
	}
	if err := killMCPProcessTree(cmd); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		process, openErr := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(childPID))
		if openErr != nil {
			return
		}
		var exitCode uint32
		queryErr := windows.GetExitCodeProcess(process, &exitCode)
		windows.CloseHandle(process)
		if queryErr != nil || exitCode != windowsStillActive {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("grandchild process %d survived Job Object termination", childPID)
}
