//go:build !windows

package acpserver

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestMCPClientCloseKillsProcessTree(t *testing.T) {
	if os.Getenv("GOCLAW_ACP_PROCESS_HELPER") == "1" {
		child := exec.Command("sleep", "30")
		if err := child.Start(); err != nil {
			os.Exit(2)
		}
		fmt.Println(child.Process.Pid)
		select {}
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMCPClientCloseKillsProcessTree$")
	cmd.Env = append(os.Environ(), "GOCLAW_ACP_PROCESS_HELPER=1")
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
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("read child PID: %v", scanner.Err())
	}
	childPID, err := strconv.Atoi(scanner.Text())
	if err != nil {
		t.Fatal(err)
	}
	if err := killMCPProcessTree(cmd); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(childPID, 0); err == syscall.ESRCH {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("grandchild process %d survived process-group termination", childPID)
}
