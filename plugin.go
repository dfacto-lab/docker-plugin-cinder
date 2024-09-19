package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"

	"github.com/docker/go-plugins-helpers/volume"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/v3/volumes"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/extensions/volumeattach"
	"github.com/gophercloud/gophercloud/pagination"
	"github.com/moby/sys/mountinfo"
)

type plugin struct {
	blockClient   *gophercloud.ServiceClient
	computeClient *gophercloud.ServiceClient
	config        *tConfig
	mutex         *sync.Mutex
}

func newPlugin(provider *gophercloud.ProviderClient, endpointOpts gophercloud.EndpointOpts, config *tConfig) (*plugin, error) {
	blockClient, err := openstack.NewBlockStorageV3(provider, endpointOpts)

	if err != nil {
		return nil, err
	}

	computeClient, err := openstack.NewComputeV2(provider, endpointOpts)

	if err != nil {
		return nil, err
	}

	if len(config.MachineID) == 0 {
		bytes, err := os.ReadFile("/etc/machine-id")
		if err != nil {
			log.WithError(err).Error("Error reading machine id")
			return nil, err
		}

		_uuid, err := uuid.FromString(strings.TrimSpace(string(bytes)))
		if err != nil {
			log.WithError(err).Error("Error parsing machine id")
			return nil, err
		}

		log.WithField("id", _uuid).Info("Machine ID detected")
		config.MachineID = _uuid.String()
	} else {
		log.WithField("id", config.MachineID).Debug("Using configured machine ID")
	}

	return &plugin{
		blockClient:   blockClient,
		computeClient: computeClient,
		config:        config,
		mutex:         &sync.Mutex{},
	}, nil
}

func (d plugin) Capabilities() *volume.CapabilitiesResponse {
	return &volume.CapabilitiesResponse{
		Capabilities: volume.Capability{Scope: "global"},
	}
}

func (d plugin) Create(r *volume.CreateRequest) error {
	logger := log.WithFields(log.Fields{"name": r.Name, "action": "create"})
	logger.Infof("Creating volume '%s' ...", r.Name)
	logger.Debugf("Create: %+v", r)

	d.mutex.Lock()
	defer d.mutex.Unlock()

	// DEFAULT SIZE IN GB
	var size = d.config.VolumeDefaultSize
	logger.Debugf("Default volume size was set to: %d", size)
	var err error

	if s, ok := r.Options["size"]; ok {
		size, err = strconv.Atoi(s)
		if err != nil {
			logger.WithError(err).Error("Error parsing size option")
			return fmt.Errorf("invalid size option: %s", err.Error())
		}
	}
	// DEFAULT type
	volumeType := d.config.VolumeDefaultType
	logger.Debugf("Default volume type was set to: %s", volumeType)
	if s, ok := r.Options["volumeType"]; ok {
		volumeType = s
	}

	vol, err := volumes.Create(d.blockClient, volumes.CreateOpts{
		Size:       size,
		Name:       r.Name,
		VolumeType: volumeType,
	}).Extract()

	if err != nil {
		logger.WithError(err).Errorf("Error creating volume: %s", err.Error())
		return err
	}
	logger.Debugf("Attaching  volume to format it")
	//we should attach it and format it
	dev, err := d.attachVolume(logger.Context, vol)
	if err != nil {
		logger.WithError(err).Errorf("Error attaching volume to format: %s", err.Error())
		return nil
	}

	format := "ext4"
	if s, ok := r.Options["filesystem"]; ok {
		format = s
	}
	logger.Debugf("Formatting  volume to %s", format)
	if err := formatFilesystem(dev, r.Name, format); err != nil {
		logger.WithError(err).Error("Formatting failed")
		return nil
	}

	//we mount the volume to create the data folder
	path, err := d.mountVolume(logger.Context, dev, vol.Name)
	if err != nil {
		logger.WithError(err).Error("Mounting volume to create data dir failed")
		return nil
	}
	uid := 0
	if s, ok := r.Options["uid"]; ok {
		_uid, err := strconv.Atoi(s)
		if err != nil {
			logger.WithError(err).Error("Error parsing uid option")
		} else {
			uid = _uid
		}
	}
	gid := 0
	if s, ok := r.Options["gid"]; ok {
		_gid, err := strconv.Atoi(s)
		if err != nil {
			logger.WithError(err).Error("Error parsing gid option")
		} else {
			gid = _gid
		}
	}
	fileMode, _ := strconv.ParseUint("0700", 8, 32)
	if s, ok := r.Options["fileMode"]; ok {
		_fileMode, err := strconv.ParseUint(s, 8, 32)
		if err != nil {
			logger.WithError(err).Error("Error parsing gid option")
		} else {
			fileMode = _fileMode
		}
	}
	subpath, err := d.createMountSubPath(logger.Context, path)
	if err != nil {
		logger.WithError(err).Error("Error creating mount sub path")
	}
	_, err = d.setPermissions(logger.Context, subpath, uid, gid, int(fileMode))
	if err != nil {
		logger.WithError(err).Error("Error setting permission on %path", path)
	}
	//unmounting volume
	err = syscall.Unmount(path, 0)
	if err != nil {
		logger.WithError(err).Errorf("Error unmount %s", path)
	}
	//getting volume information, and attachments
	vol, err = d.getByName(vol.Name)
	if err != nil {
		logger.WithError(err).Error("Error detaching volume, could not get volume attachments")
	}
	//detach volume

	if vol, err = d.detachVolume(logger.Context, vol); err != nil {
		logger.WithError(err).Errorf("Error detaching volume %s", vol.Name)
	}
	if vol, err = d.waitOnAttachmentState(logger.Context, vol, "detached"); err != nil {
		logger.WithError(err).Error("Error detaching volume")
		//return nil, err
	}
	if vol, err = d.waitOnVolumeState(logger.Context, vol, "available"); err != nil {
		logger.WithError(err).Error("Error detaching volume")
		//return nil, err
	}

	logger.WithField("id", vol.ID).Debug("Volume created")

	return nil
}

func (d plugin) Get(r *volume.GetRequest) (*volume.GetResponse, error) {
	logger := log.WithFields(log.Fields{"name": r.Name, "action": "get"})
	logger.Debugf("Get: %+v", r)

	vol, err := d.getByName(r.Name)

	if err != nil {
		logger.WithError(err).Errorf("Error retriving volume: %s", err.Error())
		return nil, err
	}

	response := &volume.GetResponse{
		Volume: &volume.Volume{
			Name:       r.Name,
			CreatedAt:  vol.CreatedAt.Format(time.RFC3339),
			Mountpoint: filepath.Join(d.config.MountDir, r.Name),
			Status: map[string]interface{}{
				"status": vol.Status,
			},
		},
	}
	logger.Debugf("Get response: %s, %s, %s", response.Volume.Mountpoint, response.Volume.Name, response.Volume.Status)
	return response, nil
}

func (d plugin) List() (*volume.ListResponse, error) {
	logger := log.WithFields(log.Fields{"action": "list"})
	logger.Debugf("List")

	var vols []*volume.Volume

	pager := volumes.List(d.blockClient, volumes.ListOpts{})
	err := pager.EachPage(func(page pagination.Page) (bool, error) {
		vList, _ := volumes.ExtractVolumes(page)

		for _, v := range vList {
			if len(v.Name) > 0 {
				vols = append(vols, &volume.Volume{
					Name:      v.Name,
					CreatedAt: v.CreatedAt.Format(time.RFC3339),
				})
			}
		}

		return true, nil
	})

	if err != nil {
		logger.WithError(err).Errorf("Error listing volume: %s", err.Error())
		return nil, err
	}

	return &volume.ListResponse{Volumes: vols}, nil
}

func (d plugin) Mount(r *volume.MountRequest) (*volume.MountResponse, error) {
	logger := log.WithFields(log.Fields{"name": r.Name, "action": "mount"})
	logger.Infof("Mounting volume '%s' ...", r.Name)
	logger.Debugf("Mount: %+v", r)

	d.mutex.Lock()
	defer d.mutex.Unlock()

	vol, err := d.getByName(r.Name)
	if err != nil {
		logger.WithError(err).Errorf("Error retriving volume: %s", err.Error())
		return nil, err
	}

	logger = logger.WithField("id", vol.ID)
	//maybe we should for status to be available or in-use, status can be reserved
	logger.Infof("Volume is in '%s' state, waiting for 'available' or 'in-use'...", vol.Status)
	if vol, err = d.waitOnVolumeState(logger.Context, vol, "available", "in-use"); err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	if vol, err = volumes.Get(d.blockClient, vol.ID).Extract(); err != nil {
		return nil, err
	}
	mounted, err := mountinfo.Mounted(d.config.MountDir)
	if err != nil {
		logger.WithError(err).Errorf("Error testing if volume is mounted on: %s", d.config.MountDir)
	}
	if mounted {
		path := filepath.Join(d.config.MountDir, r.Name)
		if len(d.config.MountSubPath) > 0 {
			path = filepath.Join(path, d.config.MountSubPath)
		}
		resp := volume.MountResponse{
			Mountpoint: path,
		}
		usageCount := useVolume(vol)
		logger.Debugf("Volume usage count is %d", usageCount)
		logger.Debug("Volume already mounted")
		return &resp, nil
	}
	if alreadyAttached(vol) {
		logger.Error("Volume attached to host but not mounted, this should not happen, or there is a race condition with another container.")
	}
	if len(vol.Attachments) > 0 {
		for i := 0; i < len(vol.Attachments); i++ {
			attachment := vol.Attachments[i]
			logger.Debugf("Attachment: Volume id %s, name %s, status: %s, attachment: %s, hostname: %s", vol.ID, vol.Name, vol.Status, attachment.Device, attachment.HostName)
			//we should check if volume is attached to current host if already attached to current host we should return the path where it's attached
		}
		if &d.config.ForceDetach != nil && !d.config.ForceDetach {
			return nil, errors.New(fmt.Sprintf("Volume id %s, name %s, status %s, is already attached to another host, force detach disabled.", vol.ID, vol.Name, vol.Status))
		}
		logger.Infof("Volume already attached, detaching first, status is: %s, attachments are: %s", vol.Status, vol.Attachments)
		if vol, err = d.detachVolume(logger.Context, vol); err != nil {
			logger.WithError(err).Error("Error detaching volume")
			return nil, err
		}

		if vol, err = d.waitOnVolumeState(logger.Context, vol, "available"); err != nil {
			logger.WithError(err).Error("Error detaching volume")
			return nil, err
		}

		for i := 0; i < len(vol.Attachments); i++ {
			attachment := vol.Attachments[i]
			logger.Debugf("Volume id %s, name %s, status: %s, attachment: %s, hostname: %s", vol.ID, vol.Name, vol.Status, attachment.Device, attachment.HostName)
		}

		if vol, err = d.waitOnAttachmentState(logger.Context, vol, "detached"); err != nil {
			logger.WithError(err).Error("Error detaching volume")
			return nil, err
		}

	}

	if vol.Status != "available" {
		logger.Debugf("Volume: %+v\n", vol)
		logger.Errorf("Invalid volume state for mounting: %s", vol.Status)
		return nil, errors.New("invalid Volume State")
	}
	//
	// Attaching block volume to compute instance
	dev, err := d.attachVolume(logger.Context, vol)
	if err != nil {
		return nil, err
	}

	//
	// Check filesystem and format if necessary
	fsType, err := getFilesystemType(dev)
	if err != nil {
		logger.WithError(err).Error("Detecting filesystem type failed")
		return nil, err
	}

	if fsType == "" {
		logger.Debug("Volume is empty, formatting")
		if err := formatExt4Filesystem(dev, r.Name); err != nil {
			logger.WithError(err).Error("Formatting failed")
			return nil, err
		}
	}

	//
	// Mount device
	path, err := d.mountVolume(logger.Context, dev, r.Name)
	if err != nil {
		return nil, err
	}

	//
	//we create "data" sub folder if it does not exist
	path, err = d.createMountSubPath(logger.Context, path)
	if err != nil {
		return nil, err
	}

	if vol, err = volumes.Get(d.blockClient, vol.ID).Extract(); err != nil {
		return nil, err
	}

	logger.Debugf("Waiting for state in-use for volume id %s, %s, %s, %s", vol.ID, vol.Name, vol.Status, vol.Attachments)

	if vol, err = d.waitOnVolumeState(logger.Context, vol, "in-use"); err != nil {
		logger.WithError(err).Error("Error mounting volume")
		return nil, err
	}
	usageCount := useVolume(vol)
	logger.Debugf("Volume usage count is %d", usageCount)
	resp := volume.MountResponse{
		Mountpoint: path,
	}

	logger.Debug("Volume successfully mounted")

	return &resp, nil
}

func (d plugin) Path(r *volume.PathRequest) (*volume.PathResponse, error) {
	logger := log.WithFields(log.Fields{"name": r.Name, "action": "path"})
	logger.Debugf("Path: %+v", r)
	path := filepath.Join(d.config.MountDir, r.Name)

	if len(d.config.MountSubPath) > 0 {
		path = filepath.Join(path, d.config.MountSubPath)
	}

	resp := volume.PathResponse{
		Mountpoint: path,
	}

	return &resp, nil
}

func (d plugin) Remove(r *volume.RemoveRequest) error {
	logger := log.WithFields(log.Fields{"name": r.Name, "action": "remove"})
	logger.Infof("Removing volume '%s' ...", r.Name)
	logger.Debugf("Remove: %+v", r)
	d.mutex.Lock()
	defer d.mutex.Unlock()
	vol, err := d.getByName(r.Name)

	if err != nil {
		logger.WithError(err).Errorf("Error retriving volume: %s", err.Error())
		return err
	}

	logger = logger.WithField("id", vol.ID)

	if len(vol.Attachments) > 0 {
		logger.Debug("Volume still attached, detaching first")
		if vol, err = d.detachVolume(logger.Context, vol); err != nil {
			logger.WithError(err).Error("Error detaching volume")
			return err
		}
	}

	logger.Debug("Deleting block volume...")

	err = volumes.Delete(d.blockClient, vol.ID, volumes.DeleteOpts{}).ExtractErr()
	if err != nil {
		logger.WithError(err).Errorf("Error deleting volume: %s", err.Error())
		return err
	}

	logger.Debug("Volume deleted")

	return nil
}

func (d plugin) Unmount(r *volume.UnmountRequest) error {
	logger := log.WithFields(log.Fields{"name": r.Name, "action": "unmount"})
	logger.Infof("Unmounting volume '%s' ...", r.Name)
	logger.Debugf("Unmount: %+v", r)

	d.mutex.Lock()
	defer d.mutex.Unlock()
	usageCount := releaseVolume(r)
	logger.Debugf("Volume usage count is %d", usageCount)
	if usageCount > 0 {
		logger.Debugf("Soft unmounting volume, still in use by other containers, usage count is %d", usageCount)
		return nil
	}

	path := filepath.Join(d.config.MountDir, r.Name)
	exists, err := isDirectoryPresent(path)
	if err != nil {
		logger.WithError(err).Error("Error checking directory stat: %s", path)
	}

	if exists {
		err = syscall.Unmount(path, 0)
		if err != nil {
			logger.WithError(err).Errorf("Error unmount %s", path)
		}
	}

	vol, err := d.getByName(r.Name)
	if err != nil {
		logger.WithError(err).Error("Error retrieving volume")
	} else {
		_, err = d.detachVolume(logger.Context, vol)
		if err != nil {
			logger.WithError(err).Error("Error detaching volume")
		}
	}
	if vol, err = d.waitOnAttachmentState(logger.Context, vol, "detached"); err != nil {
		logger.WithError(err).Error("Error detaching volume")
		//return nil, err
	}
	if vol, err = d.waitOnVolumeState(logger.Context, vol, "available"); err != nil {
		logger.WithError(err).Error("Error detaching volume")
		//return nil, err
	}
	return nil
}

func (d plugin) getByName(name string) (*volumes.Volume, error) {
	var _volume *volumes.Volume

	pager := volumes.List(d.blockClient, volumes.ListOpts{Name: name})
	err := pager.EachPage(func(page pagination.Page) (bool, error) {
		vList, err := volumes.ExtractVolumes(page)

		if err != nil {
			return false, err
		}

		for _, v := range vList {
			if v.Name == name {
				_volume = &v
				return false, nil
			}
		}

		return true, nil
	})

	if len(_volume.ID) == 0 {
		return nil, errors.New("not Found")
	}

	return _volume, err
}

func (d plugin) detachVolume(ctx context.Context, vol *volumes.Volume) (*volumes.Volume, error) {
	for _, att := range vol.Attachments {
		log.WithContext(ctx).Debugf("Detaching attachment volume %s", att.VolumeID)
		err := volumeattach.Delete(d.computeClient, att.ServerID, att.ID).ExtractErr()
		if err != nil {
			return nil, err
		}
	}
	log.WithContext(ctx).Debugf("Detached volume")
	return vol, nil
}

func (d plugin) waitOnVolumeState(ctx context.Context, vol *volumes.Volume, status ...string) (*volumes.Volume, error) {

	if slices.Contains(status, vol.Status) {
		return vol, nil
	}
	loops := d.config.Timeout * 2

	for i := 1; i <= loops; i++ {
		time.Sleep(500 * time.Millisecond)

		vol, err := volumes.Get(d.blockClient, vol.ID).Extract()
		if err != nil {
			return nil, err
		}

		if slices.Contains(status, vol.Status) {
			return vol, nil
		}
		log.WithContext(ctx).Debugf("Volume status did not become %s: %+v, waiting 500ms, %d tries left", status, vol.Status, loops-i)
	}

	return nil, fmt.Errorf("volume status did not become %s", status)
}

func (d plugin) waitOnAttachmentState(ctx context.Context, vol *volumes.Volume, status string) (*volumes.Volume, error) {
	var isDetached = true
	if status == "attached" {
		isDetached = false
	}
	loops := d.config.Timeout * 2
	if isDetached && len(vol.Attachments) == 0 {
		return vol, nil
	}
	if !isDetached && len(vol.Attachments) > 0 {
		return vol, nil
	}

	for i := 1; i <= loops; i++ {
		time.Sleep(500 * time.Millisecond)

		vol, err := volumes.Get(d.blockClient, vol.ID).Extract()

		if err != nil {
			return nil, err
		}

		if isDetached && len(vol.Attachments) == 0 {
			return vol, nil
		}
		if !isDetached && len(vol.Attachments) > 0 {
			return vol, nil
		}
		log.WithContext(ctx).Debugf("Volume state did not became %s: %+v, waiting 500ms, %d tries left", status, vol.Status, loops-i)
	}

	return nil, fmt.Errorf("volume status did become %s", status)
}

func (d plugin) mountVolume(ctx context.Context, dev string, volumeName string) (string, error) {
	path := filepath.Join(d.config.MountDir, volumeName)
	if err := os.MkdirAll(path, 0700); err != nil {
		log.WithContext(ctx).WithError(err).Error("Error creating mount directory")
		return "", err
	}

	log.WithContext(ctx).WithField("mount", path).Debug("Mounting volume %s to %s...", dev, volumeName)
	out, err := exec.Command("mount", dev, path).CombinedOutput()
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("%s", out)
		return "", errors.New(string(out))
	}
	return path, nil
}

func (d plugin) attachVolume(ctx context.Context, vol *volumes.Volume) (string, error) {
	//
	// Attaching block volume to compute instance
	log.WithContext(ctx).Debugf("Attaching volume ID %s, name %s, status %s, attachments %s", vol.ID, vol.Name, vol.Status, vol.Attachments)

	opts := volumeattach.CreateOpts{VolumeID: vol.ID}
	_, err := volumeattach.Create(d.computeClient, d.config.MachineID, opts).Extract()
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("Error attaching volume: %s", err.Error())
		return "", err
	}

	//
	// Waiting for device appearance

	// TODO : Manage Docker engine version to enable first or second syntax
	// DockerCE Version < 20.10.8 : /dev/disk/by-id/virtio-%.20s
	// DockerCE Version >= 20.10.8 : /dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_%s
	// dev := fmt.Sprintf("/dev/disk/by-id/virtio-%.20s", vol.ID)
	dev := getDeviceName(vol)
	log.WithContext(ctx).WithField("dev", dev).Debug("Waiting for device to appear...")
	err = d.waitForDevice(dev)

	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Expected block device not found")
		return "", fmt.Errorf("block device not found: %s", dev)
	}
	return dev, nil
}

func (d plugin) createMountSubPath(ctx context.Context, path string) (string, error) {
	if len(d.config.MountSubPath) > 0 {
		path = filepath.Join(path, d.config.MountSubPath)
		if err := os.MkdirAll(path, 0700); err != nil {
			log.WithContext(ctx).WithError(err).Errorf("Error creating data directory inside mounted volume: %s", path)
			return "", err
		}
	}
	return path, nil
}

func (d plugin) setPermissions(ctx context.Context, path string, uid int, gid int, fileMode int) (string, error) {
	log.WithContext(ctx).Debugf("Set mount dir permissions for path: %s, uid: %d, gid: %d, fileMode: %d", path, uid, gid, fileMode)
	if err := os.Chown(path, uid, gid); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("Unable to change gid and uid of mount directory inside volume: %s", path)
		return "", err
	}
	if err := os.Chmod(path, os.FileMode(fileMode)); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("Unable to change gid and uid of mount directory inside volume: %s", path)
		return "", err
	}
	return path, nil
}

func (d plugin) waitForDevice(dev string) error {
	_, err := os.Stat(dev)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		return nil
	}
	loops := d.config.Timeout * 2

	for i := 1; i <= loops; i++ {
		time.Sleep(500 * time.Millisecond)

		if _, err = os.Stat(dev); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		} else {
			return nil
		}
	}

	return fmt.Errorf("timeout waiting for file: %s", dev)
}
