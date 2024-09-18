package main

import (
	"errors"
	"fmt"
	"github.com/docker/go-plugins-helpers/volume"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/v3/volumes"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"time"
)

var volumeUsageCount = make(map[string]int)

func useVolume(vol *volumes.Volume) int {
	//logger := log.WithFields(log.Fields{"name": vol.Name, "action": "useVolume"})
	name := vol.Name
	val, ok := volumeUsageCount[name]
	if ok {
		volumeUsageCount[name] = val + 1
	} else {
		volumeUsageCount[name] = 1
	}
	//logger.Debugf("Volume usage count %d", volumeUsageCount[name])
	return volumeUsageCount[name]
}

func releaseVolume(vol *volume.UnmountRequest) int {
	logger := log.WithFields(log.Fields{"name": vol.Name, "action": "releaseVolume"})
	name := vol.Name
	val, ok := volumeUsageCount[name]
	if ok {
		//logger.Debugf("Volume usage count was %d", val)
		if val == 1 {
			delete(volumeUsageCount, name)
			return 0
		}
		volumeUsageCount[name] = val - 1
		return volumeUsageCount[name]
	} else {
		logger.Error("Trying to release a volume with no usage count")
	}
	return 0
}

func getFilesystemType(dev string) (string, error) {
	out, err := exec.Command("blkid", "-s", "TYPE", "-o", "value", dev).CombinedOutput()

	if err != nil {
		if len(out) == 0 {
			return "", nil
		}

		return "", errors.New(string(out))
	}

	return string(out), nil
}
func formatExt4Filesystem(dev string, label string) error {
	return formatFilesystem(dev, label, "ext4")
}

func formatFilesystem(dev string, label string, format string) error {
	out, err := exec.Command("mkfs."+format, "-L", label, dev).CombinedOutput()

	if err != nil {
		return errors.New(string(out))
	}

	return nil
}

func deviceExists(dev string) bool {
	_, err := os.Stat(dev)
	if err != nil {
		return false
	}
	return true
}

func alreadyMounted(vol *volumes.Volume) bool {
	dev := getDeviceName(vol)
	//maybe we could check if status is in use
	//if vol.Status != "in-use" {
	//    return false
	//}
	return deviceExists(dev)
}

func getDeviceName(vol *volumes.Volume) string {
	// TODO : Manage Docker engine version to enable first or second syntax
	// DockerCE Version < 20.10.8 : /dev/disk/by-id/virtio-%.20s
	// DockerCE Version >= 20.10.8 : /dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_%s
	// dev := fmt.Sprintf("/dev/disk/by-id/virtio-%.20s", vol.ID)
	dev := fmt.Sprintf("/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_%s", vol.ID)
	return dev
}

func waitForDevice(dev string) error {
	_, err := os.Stat(dev)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		return nil
	}

	for i := 1; i <= 10; i++ {
		time.Sleep(500 * time.Millisecond)

		if _, err = os.Stat(dev); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		} else {
			return nil
		}
	}

	return fmt.Errorf("Timeout waiting for file: %s", dev)
}

func isDirectoryPresent(path string) (bool, error) {
	stat, err := os.Stat(path)

	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return stat.IsDir(), nil
	}
}
