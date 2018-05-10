/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/cloudprovider"
	kubeletapis "k8s.io/kubernetes/pkg/kubelet/apis"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"

	"github.com/golang/glog"
	computebeta "google.golang.org/api/compute/v0.beta"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/cloudprovider/providers/gce/cloud"
	"k8s.io/kubernetes/pkg/cloudprovider/providers/gce/cloud/meta"
	"k8s.io/kubernetes/pkg/features"
)

type DiskType string

const (
	DiskTypeSSD      = "pd-ssd"
	DiskTypeStandard = "pd-standard"

	diskTypeDefault               = DiskTypeStandard
	diskTypeURITemplateSingleZone = "%s/zones/%s/diskTypes/%s"   // {gce.projectID}/zones/{disk.Zone}/diskTypes/{disk.Type}"
	diskTypeURITemplateRegional   = "%s/regions/%s/diskTypes/%s" // {gce.projectID}/regions/{disk.Region}/diskTypes/{disk.Type}"
	diskTypePersistent            = "PERSISTENT"

	diskSourceURITemplateSingleZone = "%s/zones/%s/disks/%s"   // {gce.projectID}/zones/{disk.Zone}/disks/{disk.Name}"
	diskSourceURITemplateRegional   = "%s/regions/%s/disks/%s" //{gce.projectID}/regions/{disk.Region}/disks/repd"

	replicaZoneURITemplateSingleZone = "%s/zones/%s" // {gce.projectID}/zones/{disk.Zone}

	deviceNameRegionalSuffix  = "_regional" // TODO (verult) can this be passed in another way? Why do both gce_disks and gce_util need this param?
)

type diskServiceManager interface {
	// Creates a new persistent disk on GCE with the given disk spec.
	CreateDiskOnCloudProvider(
		name string,
		sizeGb int64,
		tagsStr string,
		diskType string,
		zone string) error

	// Creates a new regional persistent disk on GCE with the given disk spec.
	CreateRegionalDiskOnCloudProvider(
		name string,
		sizeGb int64,
		tagsStr string,
		diskType string,
		zones sets.String) error

	// Deletes the persistent disk from GCE with the given diskName.
	DeleteDiskOnCloudProvider(zone string, disk string) error

	// Deletes the regional persistent disk from GCE with the given diskName.
	DeleteRegionalDiskOnCloudProvider(diskName string) error

	// Attach a persistent disk on GCE with the given disk spec to the specified instance.
	AttachDiskOnCloudProvider(
		disk *GCEDisk,
		deviceName string,
		readWrite string,
		instanceZone string,
		instanceName string) error

	// Detach a persistent disk on GCE with the given disk spec from the specified instance.
	DetachDiskOnCloudProvider(
		instanceZone string,
		instanceName string,
		deviceName string) error

	ResizeDiskOnCloudProvider(disk *GCEDisk, sizeGb int64, zone string) error
	RegionalResizeDiskOnCloudProvider(disk *GCEDisk, sizeGb int64) error

	// Gets the persistent disk from GCE with the given diskName.
	GetDiskFromCloudProvider(zone string, diskName string) (*GCEDisk, error)

	// Gets the regional persistent disk from GCE with the given diskName.
	GetRegionalDiskFromCloudProvider(diskName string) (*GCEDisk, error)
}

type gceServiceManager struct {
	gce *GCECloud
}

var _ diskServiceManager = &gceServiceManager{}

func (manager *gceServiceManager) CreateDiskOnCloudProvider(
	name string,
	sizeGb int64,
	tagsStr string,
	diskType string,
	zone string) error {
	diskTypeURI, err := manager.getDiskTypeURI(
		manager.gce.region /* diskRegion */, singleZone{zone}, diskType, false /* useBetaAPI */)
	if err != nil {
		return err
	}

	diskToCreateV1 := &compute.Disk{
		Name:        name,
		SizeGb:      sizeGb,
		Description: tagsStr,
		Type:        diskTypeURI,
	}

	ctx, cancel := cloud.ContextWithCallTimeout()
	defer cancel()
	return manager.gce.c.Disks().Insert(ctx, meta.ZonalKey(name, zone), diskToCreateV1)
}

func (manager *gceServiceManager) CreateRegionalDiskOnCloudProvider(
	name string,
	sizeGb int64,
	tagsStr string,
	diskType string,
	replicaZones sets.String) error {

	if !utilfeature.DefaultFeatureGate.Enabled(features.GCERegionalPersistentDisk) {
		return fmt.Errorf("the regional PD feature is only available with the %s Kubernetes feature gate enabled", features.GCERegionalPersistentDisk)
	}

	diskTypeURI, err := manager.getDiskTypeURI(
		manager.gce.region /* diskRegion */, multiZone{replicaZones}, diskType, true /* useBetaAPI */)
	if err != nil {
		return err
	}
	fullyQualifiedReplicaZones := []string{}
	for _, replicaZone := range replicaZones.UnsortedList() {
		fullyQualifiedReplicaZones = append(
			fullyQualifiedReplicaZones, manager.getReplicaZoneURI(replicaZone, true))
	}

	diskToCreateBeta := &computebeta.Disk{
		Name:         name,
		SizeGb:       sizeGb,
		Description:  tagsStr,
		Type:         diskTypeURI,
		ReplicaZones: fullyQualifiedReplicaZones,
	}

	ctx, cancel := cloud.ContextWithCallTimeout()
	defer cancel()
	return manager.gce.c.BetaRegionDisks().Insert(ctx, meta.RegionalKey(name, manager.gce.region), diskToCreateBeta)
}

func (manager *gceServiceManager) AttachDiskOnCloudProvider(
	disk *GCEDisk,
	deviceName string,
	readWrite string,
	instanceZone string,
	instanceName string) error {
	source, err := manager.getDiskSourceURI(disk)
	if err != nil {
		return err
	}

	attachedDiskV1 := &compute.AttachedDisk{
		DeviceName: deviceName,
		Kind:       disk.Kind,
		Mode:       readWrite,
		Source:     source,
		Type:       diskTypePersistent,
	}

	ctx, cancel := cloud.ContextWithCallTimeout()
	defer cancel()
	return manager.gce.c.Instances().AttachDisk(ctx, meta.ZonalKey(instanceName, instanceZone), attachedDiskV1)
}

func (manager *gceServiceManager) DetachDiskOnCloudProvider(
	instanceZone string,
	instanceName string,
	deviceName string) error {
	ctx, cancel := cloud.ContextWithCallTimeout()
	defer cancel()
	return manager.gce.c.Instances().DetachDisk(ctx, meta.ZonalKey(instanceName, instanceZone), deviceName)
}

func (manager *gceServiceManager) GetDiskFromCloudProvider(
	zone string,
	diskName string) (*GCEDisk, error) {
	if zone == "" {
		return nil, fmt.Errorf("Can not fetch disk %q. Zone is empty.", diskName)
	}

	if diskName == "" {
		return nil, fmt.Errorf("Can not fetch disk. Zone is specified (%q). But disk name is empty.", zone)
	}

	ctx, cancel := cloud.ContextWithCallTimeout()
	defer cancel()
	diskStable, err := manager.gce.c.Disks().Get(ctx, meta.ZonalKey(diskName, zone))
	if err != nil {
		return nil, err
	}

	zoneInfo := singleZone{strings.TrimSpace(lastComponent(diskStable.Zone))}
	if zoneInfo.zone == "" {
		zoneInfo = singleZone{zone}
	}

	region, err := manager.getRegionFromZone(zoneInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to extract region from zone for %q/%q err=%v", zone, diskName, err)
	}

	return &GCEDisk{
		ZoneInfo: zoneInfo,
		Region:   region,
		Name:     diskStable.Name,
		Kind:     diskStable.Kind,
		Type:     diskStable.Type,
		SizeGb:   diskStable.SizeGb,
	}, nil
}

func (manager *gceServiceManager) GetRegionalDiskFromCloudProvider(
	diskName string) (*GCEDisk, error) {

	if !utilfeature.DefaultFeatureGate.Enabled(features.GCERegionalPersistentDisk) {
		return nil, fmt.Errorf("the regional PD feature is only available with the %s Kubernetes feature gate enabled", features.GCERegionalPersistentDisk)
	}

	ctx, cancel := cloud.ContextWithCallTimeout()
	defer cancel()
	diskBeta, err := manager.gce.c.BetaRegionDisks().Get(ctx, meta.RegionalKey(diskName, manager.gce.region))
	if err != nil {
		return nil, err
	}

	zones := sets.NewString()
	for _, zoneURI := range diskBeta.ReplicaZones {
		zones.Insert(lastComponent(zoneURI))
	}

	return &GCEDisk{
		ZoneInfo: multiZone{zones},
		Region:   lastComponent(diskBeta.Region),
		Name:     diskBeta.Name,
		Kind:     diskBeta.Kind,
		Type:     diskBeta.Type,
		SizeGb:   diskBeta.SizeGb,
	}, nil
}

func (manager *gceServiceManager) DeleteDiskOnCloudProvider(
	zone string,
	diskName string) error {
	ctx, cancel := cloud.ContextWithCallTimeout()
	defer cancel()
	return manager.gce.c.Disks().Delete(ctx, meta.ZonalKey(diskName, zone))
}

func (manager *gceServiceManager) DeleteRegionalDiskOnCloudProvider(
	diskName string) error {

	if !utilfeature.DefaultFeatureGate.Enabled(features.GCERegionalPersistentDisk) {
		return fmt.Errorf("the regional PD feature is only available with the %s Kubernetes feature gate enabled", features.GCERegionalPersistentDisk)
	}

	ctx, cancel := cloud.ContextWithCallTimeout()
	defer cancel()
	return manager.gce.c.BetaRegionDisks().Delete(ctx, meta.RegionalKey(diskName, manager.gce.region))
}

func (manager *gceServiceManager) getDiskSourceURI(disk *GCEDisk) (string, error) {
	getProjectsAPIEndpoint := manager.getProjectsAPIEndpoint()

	switch zoneInfo := disk.ZoneInfo.(type) {
	case singleZone:
		if zoneInfo.zone == "" || disk.Region == "" {
			// Unexpected, but sanity-check
			return "", fmt.Errorf("PD does not have zone/region information: %#v", disk)
		}

		return getProjectsAPIEndpoint + fmt.Sprintf(
			diskSourceURITemplateSingleZone,
			manager.gce.projectID,
			zoneInfo.zone,
			disk.Name), nil
	case multiZone:
		if zoneInfo.replicaZones == nil || zoneInfo.replicaZones.Len() <= 0 {
			// Unexpected, but sanity-check
			return "", fmt.Errorf("PD is regional but does not have any replicaZones specified: %v", disk)
		}
		return getProjectsAPIEndpoint + fmt.Sprintf(
			diskSourceURITemplateRegional,
			manager.gce.projectID,
			disk.Region,
			disk.Name), nil
	case nil:
		// Unexpected, but sanity-check
		return "", fmt.Errorf("PD did not have ZoneInfo: %v", disk)
	default:
		// Unexpected, but sanity-check
		return "", fmt.Errorf("disk.ZoneInfo has unexpected type %T", zoneInfo)
	}
}

func (manager *gceServiceManager) getDiskTypeURI(
	diskRegion string, diskZoneInfo zoneType, diskType string, useBetaAPI bool) (string, error) {

	var getProjectsAPIEndpoint string
	if useBetaAPI {
		getProjectsAPIEndpoint = manager.getProjectsAPIEndpointBeta()
	} else {
		getProjectsAPIEndpoint = manager.getProjectsAPIEndpoint()
	}

	switch zoneInfo := diskZoneInfo.(type) {
	case singleZone:
		if zoneInfo.zone == "" {
			return "", fmt.Errorf("zone is empty: %v", zoneInfo)
		}

		return getProjectsAPIEndpoint + fmt.Sprintf(
			diskTypeURITemplateSingleZone,
			manager.gce.projectID,
			zoneInfo.zone,
			diskType), nil
	case multiZone:
		if zoneInfo.replicaZones == nil || zoneInfo.replicaZones.Len() <= 0 {
			return "", fmt.Errorf("zoneInfo is regional but does not have any replicaZones specified: %v", zoneInfo)
		}
		return getProjectsAPIEndpoint + fmt.Sprintf(
			diskTypeURITemplateRegional,
			manager.gce.projectID,
			diskRegion,
			diskType), nil
	case nil:
		return "", fmt.Errorf("zoneInfo nil")
	default:
		return "", fmt.Errorf("zoneInfo has unexpected type %T", zoneInfo)
	}
}

func (manager *gceServiceManager) getReplicaZoneURI(zone string, useBetaAPI bool) string {
	var getProjectsAPIEndpoint string
	if useBetaAPI {
		getProjectsAPIEndpoint = manager.getProjectsAPIEndpointBeta()
	} else {
		getProjectsAPIEndpoint = manager.getProjectsAPIEndpoint()
	}

	return getProjectsAPIEndpoint + fmt.Sprintf(
		replicaZoneURITemplateSingleZone,
		manager.gce.projectID,
		zone)
}

func (manager *gceServiceManager) getRegionFromZone(zoneInfo zoneType) (string, error) {
	var zone string
	switch zoneInfo := zoneInfo.(type) {
	case singleZone:
		if zoneInfo.zone == "" {
			// Unexpected, but sanity-check
			return "", fmt.Errorf("PD is single zone, but zone is not specified: %#v", zoneInfo)
		}

		zone = zoneInfo.zone
	case multiZone:
		if zoneInfo.replicaZones == nil || zoneInfo.replicaZones.Len() <= 0 {
			// Unexpected, but sanity-check
			return "", fmt.Errorf("PD is regional but does not have any replicaZones specified: %v", zoneInfo)
		}

		zone = zoneInfo.replicaZones.UnsortedList()[0]
	case nil:
		// Unexpected, but sanity-check
		return "", fmt.Errorf("zoneInfo is nil")
	default:
		// Unexpected, but sanity-check
		return "", fmt.Errorf("zoneInfo has unexpected type %T", zoneInfo)
	}

	region, err := GetGCERegion(zone)
	if err != nil {
		glog.Warningf("failed to parse GCE region from zone %q: %v", zone, err)
		region = manager.gce.region
	}

	return region, nil
}

func (manager *gceServiceManager) ResizeDiskOnCloudProvider(disk *GCEDisk, sizeGb int64, zone string) error {
	resizeServiceRequest := &compute.DisksResizeRequest{
		SizeGb: sizeGb,
	}

	ctx, cancel := cloud.ContextWithCallTimeout()
	defer cancel()
	return manager.gce.c.Disks().Resize(ctx, meta.ZonalKey(disk.Name, zone), resizeServiceRequest)
}

func (manager *gceServiceManager) RegionalResizeDiskOnCloudProvider(disk *GCEDisk, sizeGb int64) error {
	if !utilfeature.DefaultFeatureGate.Enabled(features.GCERegionalPersistentDisk) {
		return fmt.Errorf("the regional PD feature is only available with the %s Kubernetes feature gate enabled", features.GCERegionalPersistentDisk)
	}

	resizeServiceRequest := &computebeta.RegionDisksResizeRequest{
		SizeGb: sizeGb,
	}

	ctx, cancel := cloud.ContextWithCallTimeout()
	defer cancel()
	return manager.gce.c.BetaRegionDisks().Resize(ctx, meta.RegionalKey(disk.Name, disk.Region), resizeServiceRequest)
}

// TODO (verult) !!! Need a generic disk key type that works for regional PV, regular PV, and inline volumes.
// Disks is interface for manipulation with GCE PDs.
type Disks interface {
	// AttachDisk attaches given disk to the node with the specified NodeName.
	// Current instance is used when instanceID is empty string.
	AttachDisk(diskName string, nodeName types.NodeName, deviceName string, readOnly bool, regional bool) error

	// TODO (verult) Shouldn't pass devicePath as param - only cloud provider knows about device path.
	// DetachDisk detaches given disk to the node with the specified NodeName.
	// Current instance is used when nodeName is empty string.
	DetachDisk(deviceName string, nodeName types.NodeName) error

	// TODO (verult) identify by device name
	// DiskIsAttached checks if a disk is attached to the node with the specified NodeName.
	DiskIsAttached(deviceName string, nodeName types.NodeName) (bool, error)

	// TODO (verult) identify by device names
	// DisksAreAttached is a batch function to check if a list of disks are attached
	// to the node with the specified NodeName.
	DisksAreAttached(deviceNames []string, nodeName types.NodeName) (map[string]bool, error)

	// CreateDisk creates a new PD with given properties. Tags are serialized
	// as JSON into Description field.
	CreateDisk(name string, diskType string, zone string, sizeGb int64, tags map[string]string) error

	// CreateRegionalDisk creates a new Regional Persistent Disk, with the
	// specified properties, replicated to the specified zones. Tags are
	// serialized as JSON into Description field.
	CreateRegionalDisk(name string, diskType string, replicaZones sets.String, sizeGb int64, tags map[string]string) error

	// DeleteDisk deletes PD. Zone can be empty, in which case the operation searches through all available zones.
	DeleteDisk(name string, zone string) error

	// TODO (verult) comment
	DeleteRegionalDisk(name string) error

	// TODO (verult) Need to identify based on zone as well.
	// ResizeDisk resizes PD and returns new disk size
	ResizeDisk(diskToResize string, zoneSet sets.String, oldSize resource.Quantity, newSize resource.Quantity) (resource.Quantity, error)

	// GetAutoLabelsForPD returns labels to apply to PersistentVolume
	// representing this PD, namely failure domain and zone.
	// zone can be provided to specify the zone for the PD,
	// if empty all managed zones will be searched.
	GetAutoLabelsForPD(name string, zone string, regional *bool) (map[string]string, error)
}

// GCECloud implements Disks.
var _ Disks = (*GCECloud)(nil)

// GCECloud implements PVLabeler.
var _ cloudprovider.PVLabeler = (*GCECloud)(nil)

type GCEDisk struct {
	ZoneInfo zoneType // TODO (verult) is this still necessary?
	Region   string
	Name     string
	Kind     string
	Type     string
	SizeGb   int64
}

type zoneType interface {
	isZoneType()
}

type multiZone struct {
	replicaZones sets.String
}

type singleZone struct {
	zone string
}

func (m multiZone) isZoneType()  {}
func (s singleZone) isZoneType() {}

func newDiskMetricContextZonal(request, region, zone string) *metricContext {
	return newGenericMetricContext("disk", request, region, zone, computeV1Version)
}

func newDiskMetricContextRegional(request, region string) *metricContext {
	return newGenericMetricContext("disk", request, region, unusedMetricLabel, computeV1Version)
}

func (gce *GCECloud) GetLabelsForVolume(ctx context.Context, pv *v1.PersistentVolume) (map[string]string, error) {
	// Ignore any volumes that are being provisioned
	if pv.Spec.GCEPersistentDisk.PDName == volume.ProvisionedVolumeName {
		return nil, nil
	}

	// If the zone is already labeled, honor the hint
	zone := pv.Labels[kubeletapis.LabelZoneFailureDomain]

	labels, err := gce.GetAutoLabelsForPD(pv.Spec.GCEPersistentDisk.PDName, zone, nil /* regional */)
	if err != nil {
		return nil, err
	}

	return labels, nil
}

func (gce *GCECloud) AttachDisk(diskName string, nodeName types.NodeName, deviceName string, readOnly bool, regional bool) error {
	instanceName := mapNodeNameToInstanceName(nodeName)
	instance, err := gce.getInstanceByName(instanceName)
	if err != nil {
		return fmt.Errorf("error getting instance %q", instanceName)
	}

	// Try fetching as regional PD
	var disk *GCEDisk
	var mc *metricContext
	if regional && utilfeature.DefaultFeatureGate.Enabled(features.GCERegionalPersistentDisk) {
		disk, err = gce.getRegionalDiskByName(diskName)
		if err != nil {
			return err
		}
		mc = newDiskMetricContextRegional("attach", gce.region)
	} else {
		disk, err = gce.getDiskByName(diskName, instance.Zone)
		if err != nil {
			return err
		}
		mc = newDiskMetricContextZonal("attach", gce.region, instance.Zone)
	}

	readWrite := "READ_WRITE"
	if readOnly {
		readWrite = "READ_ONLY"
	}

	return mc.Observe(gce.manager.AttachDiskOnCloudProvider(disk, deviceName, readWrite, instance.Zone, instance.Name))
}

func (gce *GCECloud) DetachDisk(deviceName string, nodeName types.NodeName) error {
	instanceName := mapNodeNameToInstanceName(nodeName)
	inst, err := gce.getInstanceByName(instanceName)
	if err != nil {
		if err == cloudprovider.InstanceNotFound {
			// If instance no longer exists, safe to assume volume is not attached.
			glog.Warningf(
				"Instance %q does not exist. DetachDisk will assume PD %q is not attached to it.",
				instanceName,
				deviceName)
			return nil
		}

		return fmt.Errorf("error getting instance %q", instanceName)
	}

	mc := newDiskMetricContextZonal("detach", gce.region, inst.Zone)
	return mc.Observe(gce.manager.DetachDiskOnCloudProvider(inst.Zone, inst.Name, deviceName))
}

func (gce *GCECloud) DiskIsAttached(deviceName string, nodeName types.NodeName) (bool, error) {
	instanceName := mapNodeNameToInstanceName(nodeName)
	instance, err := gce.getInstanceByName(instanceName)
	if err != nil {
		if err == cloudprovider.InstanceNotFound {
			// If instance no longer exists, safe to assume volume is not attached.
			glog.Warningf(
				"Instance %q does not exist. DiskIsAttached will assume PD %q is not attached to it.",
				instanceName,
				deviceName)
			return false, nil
		}

		return false, err
	}

	for _, disk := range instance.Disks {
		if disk.DeviceName == deviceName {
			// Disk is still attached to node
			return true, nil
		}
	}

	return false, nil
}

func (gce *GCECloud) DisksAreAttached(deviceNames []string, nodeName types.NodeName) (map[string]bool, error) {
	attached := make(map[string]bool)
	for _, deviceName := range deviceNames {
		attached[deviceName] = false
	}
	instanceName := mapNodeNameToInstanceName(nodeName)
	instance, err := gce.getInstanceByName(instanceName)
	if err != nil {
		if err == cloudprovider.InstanceNotFound {
			// If instance no longer exists, safe to assume volume is not attached.
			glog.Warningf(
				"Instance %q does not exist. DisksAreAttached will assume PD %v are not attached to it.",
				instanceName,
				deviceNames)
			return attached, nil
		}

		return attached, err
	}

	for _, instanceDisk := range instance.Disks {
		for _, deviceName := range deviceNames {
			if instanceDisk.DeviceName == deviceName {
				// Disk is still attached to node
				attached[deviceName] = true
			}
		}
	}

	return attached, nil
}

// CreateDisk creates a new Persistent Disk, with the specified name &
// size, in the specified zone. It stores specified tags encoded in
// JSON in Description field.
func (gce *GCECloud) CreateDisk(
	name string, diskType string, zone string, sizeGb int64, tags map[string]string) error {
	// Do not allow creation of PDs in zones that are do not have nodes. Such PDs
	// are not currently usable.
	curZones, err := gce.GetAllCurrentZones()
	if err != nil {
		return err
	}
	if !curZones.Has(zone) {
		return fmt.Errorf("kubernetes does not have a node in zone %q", zone)
	}

	tagsStr, err := gce.encodeDiskTags(tags)
	if err != nil {
		return err
	}

	diskType, err = getDiskType(diskType)
	if err != nil {
		return err
	}

	mc := newDiskMetricContextZonal("create", gce.region, zone)

	err = gce.manager.CreateDiskOnCloudProvider(
		name, sizeGb, tagsStr, diskType, zone)

	mc.Observe(err)
	if isGCEError(err, "alreadyExists") {
		glog.Warningf("GCE PD %q already exists, reusing", name)
		return nil
	}
	return err
}

// CreateRegionalDisk creates a new Regional Persistent Disk, with the specified
// name & size, replicated to the specified zones. It stores specified tags
// encoded in JSON in Description field.
func (gce *GCECloud) CreateRegionalDisk(
	name string, diskType string, replicaZones sets.String, sizeGb int64, tags map[string]string) error {

	// Do not allow creation of PDs in zones that are do not have nodes. Such PDs
	// are not currently usable. This functionality should be reverted to checking
	// against managed zones if we want users to be able to create RegionalDisks
	// in zones where there are no nodes
	curZones, err := gce.GetAllCurrentZones()
	if err != nil {
		return err
	}
	if !curZones.IsSuperset(replicaZones) {
		return fmt.Errorf("kubernetes does not have nodes in specified zones: %q. Zones that contain nodes: %q", replicaZones.Difference(curZones), curZones)
	}

	tagsStr, err := gce.encodeDiskTags(tags)
	if err != nil {
		return err
	}

	diskType, err = getDiskType(diskType)
	if err != nil {
		return err
	}

	mc := newDiskMetricContextRegional("create", gce.region)

	err = gce.manager.CreateRegionalDiskOnCloudProvider(
		name, sizeGb, tagsStr, diskType, replicaZones)

	mc.Observe(err)
	if isGCEError(err, "alreadyExists") {
		glog.Warningf("GCE PD %q already exists, reusing", name)
		return nil
	}
	return err
}

func getDiskType(diskType string) (string, error) {
	switch diskType {
	case DiskTypeSSD, DiskTypeStandard:
		return diskType, nil
	case "":
		return diskTypeDefault, nil
	default:
		return "", fmt.Errorf("invalid GCE disk type %q", diskType)
	}
}

// TODO (verult) simplify
func (gce *GCECloud) DeleteDisk(name string, zone string) error {
	var err error
	if zone == "" {
		err = gce.doDeleteDisk(name) // TODO (verult) refactor, change function name
	} else {
		disk, err := gce.getDiskByName(name, zone)
		if err != nil {
			return err
		}

		mc := newDiskMetricContextZonal("delete", disk.Region, zone)
		err = gce.manager.DeleteDiskOnCloudProvider(zone, disk.Name)
		if err != nil {
			return mc.Observe(err)
		}
	}

	if isGCEError(err, "resourceInUseByAnotherResource") {
		return volume.NewDeletedVolumeInUseError(err.Error())
	}

	if err == cloudprovider.DiskNotFound {
		return nil
	}
	return err
}

func (gce *GCECloud) DeleteRegionalDisk(name string) error {
	disk, err := gce.getRegionalDiskByName(name)
	if err != nil {
		if err == cloudprovider.DiskNotFound {
			return nil
		}
		return err
	}

	mc := newDiskMetricContextRegional("delete", disk.Region)
	err = gce.manager.DeleteRegionalDiskOnCloudProvider(disk.Name)
	if err != nil {
		return mc.Observe(err)
	}

	if isGCEError(err, "resourceInUseByAnotherResource") {
		return volume.NewDeletedVolumeInUseError(err.Error())
	}

	return err
}

// ResizeDisk expands given disk and returns new disk size
func (gce *GCECloud) ResizeDisk(diskToResize string, zoneSet sets.String, oldSize resource.Quantity, newSize resource.Quantity) (resource.Quantity, error) {
	var disk *GCEDisk
	var err error
	switch zoneSet.Len() {
	case 2:
		disk, err = gce.findRegionalDiskByName(diskToResize)
	case 1:
		disk, err = gce.findDiskByName(diskToResize, zoneSet.UnsortedList()[0])
	case 0:
		disk, err = gce.GetDiskByNameUnknownZone(diskToResize)
	}
	if err != nil {
		return oldSize, err
	}

	requestBytes := newSize.Value()
	// GCE resizes in chunks of GBs (not GiB)
	requestGB := volumeutil.RoundUpSize(requestBytes, 1000*1000*1000)
	newSizeQuant := resource.MustParse(fmt.Sprintf("%dG", requestGB))

	// If disk is already of size equal or greater than requested size, we simply return
	if disk.SizeGb >= requestGB {
		return newSizeQuant, nil
	}

	var mc *metricContext

	switch zoneInfo := disk.ZoneInfo.(type) {
	case singleZone:
		mc = newDiskMetricContextZonal("resize", disk.Region, zoneInfo.zone)
		err := gce.manager.ResizeDiskOnCloudProvider(disk, requestGB, zoneInfo.zone)

		if err != nil {
			return oldSize, mc.Observe(err)
		} else {
			return newSizeQuant, mc.Observe(err)
		}
	case multiZone:
		if !utilfeature.DefaultFeatureGate.Enabled(features.GCERegionalPersistentDisk) {
			return oldSize, fmt.Errorf("disk.ZoneInfo has unexpected type %T", zoneInfo)
		}

		mc = newDiskMetricContextRegional("resize", disk.Region)
		err := gce.manager.RegionalResizeDiskOnCloudProvider(disk, requestGB)

		if err != nil {
			return oldSize, mc.Observe(err)
		} else {
			return newSizeQuant, mc.Observe(err)
		}
	case nil:
		return oldSize, fmt.Errorf("PD has nil ZoneInfo: %v", disk)
	default:
		return oldSize, fmt.Errorf("disk.ZoneInfo has unexpected type %T", zoneInfo)
	}
}

// TODO (verult) Update comments (RePD is never searched if input is ambiguous)
// TODO (verult) caller could provide either zone or regional hints, but not both. Could there be a better way?
// TODO (verult) TEST!!!!! I don't have confidence in this piece of code
// Builds the labels that should be automatically added to a PersistentVolume backed by a GCE PD
// Specifically, this builds FailureDomain (zone) and Region labels.
// The PersistentVolumeLabel admission controller calls this and adds the labels when a PV is created.
// If zone is specified, the volume will only be found in the specified zone,
// otherwise all managed zones will be searched.
func (gce *GCECloud) GetAutoLabelsForPD(name string, zone string, regional *bool) (map[string]string, error) {
	// If regional == true (zone == ""), only scan for regional disk
	// If regional == false (zone == ""), only scan for regular disk

	// If regional == nil && zone == "", scan for both regional and regular disks.
	// If regional == nil && zone != "", scan for regional disk or regular disk depending on zone

	var disk *GCEDisk
	var err error

	if regional == nil && zone == "" {
		// get regional disk and scan regular disks

		// For regional PDs this is fine, but for zonal PDs we would like as far
		// as possible to avoid this case, because GCE doesn't guarantee that
		// volumes are uniquely named per region, just per zone. However,
		// creation of GCE PDs was originally done only by name, so we have to
		// continue to support that.
		// However, wherever possible the zone should be passed (and it is
		// passed for most cases that we can control, e.g. dynamic volume
		// provisioning).
		disk, err = gce.GetDiskByNameUnknownZone(name)
		if err != nil && err != cloudprovider.DiskNotFound {
			return nil, err
		}

		// TODO (verult) is flag gate necessary?
		if utilfeature.DefaultFeatureGate.Enabled(features.GCERegionalPersistentDisk) {
			regionalDisk, err := gce.getRegionalDiskByName(name)

			if disk != nil && regionalDisk != nil {
				return nil, fmt.Errorf("Both regional and regular PDs were found with name %q", name)
			} else if disk == nil && regionalDisk == nil {
				return nil, err
			} else if disk == nil && regionalDisk != nil {
				disk = regionalDisk
			} // else disk != nil && regionalDisk == nil, use the previously found regular disk.
		}

	} else if regional == nil && zone != "" {
		// get either regional disk or regular disk depending on zone count

		zoneSet, zoneErr := volumeutil.LabelZonesToSet(zone)
		if zoneErr != nil {
			glog.Warningf("Failed to parse zone field: %q. Will use raw field.", zone)
		}

		if len(zoneSet) > 1 {
			disk, err = gce.getRegionalDiskByName(name)
		} else { // len(zoneSet) == 1
			disk, err = gce.getDiskByName(name, zone)
		}

	} else if *regional && zone == "" {
		// get regional disk
		disk, err = gce.getRegionalDiskByName(name)
	} else if *regional && zone != "" {
		// Programming error
		return nil, fmt.Errorf("zone and regional hints can't both be specified.")
	} else if !*regional && zone == "" {
		// scan for regular disk

		// For zonal PDs we would like as far
		// as possible to avoid this case, because GCE doesn't guarantee that
		// volumes are uniquely named per region, just per zone. However,
		// creation of GCE PDs was originally done only by name, so we have to
		// continue to support that.
		// However, wherever possible the zone should be passed (and it is
		// passed for most cases that we can control, e.g. dynamic volume
		// provisioning).
		disk, err = gce.GetDiskByNameUnknownZone(name)
	} else { // !*regional && zone != ""
		// Programming error
		return nil, fmt.Errorf("zone and regional hints can't both be specified.")
	}

	if err != nil {
		return nil, err
	}

	labels := make(map[string]string)
	switch zoneInfo := disk.ZoneInfo.(type) {
	case singleZone:
		if zoneInfo.zone == "" || disk.Region == "" {
			// Unexpected, but sanity-check
			return nil, fmt.Errorf("PD did not have zone/region information: %v", disk)
		}
		labels[kubeletapis.LabelZoneFailureDomain] = zoneInfo.zone
		labels[kubeletapis.LabelZoneRegion] = disk.Region
	case multiZone:
		if zoneInfo.replicaZones == nil || zoneInfo.replicaZones.Len() <= 0 {
			// Unexpected, but sanity-check
			return nil, fmt.Errorf("PD is regional but does not have any replicaZones specified: %v", disk)
		}
		labels[kubeletapis.LabelZoneFailureDomain] =
			volumeutil.ZonesSetToLabelValue(zoneInfo.replicaZones)
		labels[kubeletapis.LabelZoneRegion] = disk.Region
	case nil:
		// Unexpected, but sanity-check
		return nil, fmt.Errorf("PD did not have ZoneInfo: %v", disk)
	default:
		// Unexpected, but sanity-check
		return nil, fmt.Errorf("disk.ZoneInfo has unexpected type %T", zoneInfo)
	}

	return labels, nil
}

// Returns a GCEDisk for the disk, if it is found in the specified zone.
// If not found, returns (nil, nil)
func (gce *GCECloud) findDiskByName(diskName string, zone string) (*GCEDisk, error) {
	mc := newDiskMetricContextZonal("get", gce.region, zone)
	disk, err := gce.manager.GetDiskFromCloudProvider(zone, diskName)
	if err == nil {
		return disk, mc.Observe(nil)
	}
	if !isHTTPErrorCode(err, http.StatusNotFound) {
		return nil, mc.Observe(err)
	}
	return nil, mc.Observe(nil)
}

// Like findDiskByName, but returns an error if the disk is not found
func (gce *GCECloud) getDiskByName(diskName string, zone string) (*GCEDisk, error) {
	disk, err := gce.findDiskByName(diskName, zone)
	if disk == nil && err == nil {
		return nil, fmt.Errorf("GCE persistent disk not found: diskName=%q zone=%q", diskName, zone)
	}
	return disk, err
}

// Returns a GCEDisk for the regional disk, if it is found.
// If not found, returns (nil, nil)
func (gce *GCECloud) findRegionalDiskByName(diskName string) (*GCEDisk, error) {
	mc := newDiskMetricContextRegional("get", gce.region)
	disk, err := gce.manager.GetRegionalDiskFromCloudProvider(diskName)
	if err == nil {
		return disk, mc.Observe(nil)
	}
	if !isHTTPErrorCode(err, http.StatusNotFound) {
		return nil, mc.Observe(err)
	}
	return nil, mc.Observe(nil)
}

// Like findRegionalDiskByName, but returns an error if the disk is not found
func (gce *GCECloud) getRegionalDiskByName(diskName string) (*GCEDisk, error) {
	disk, err := gce.findRegionalDiskByName(diskName)
	if disk == nil && err == nil {
		return nil, fmt.Errorf("GCE regional persistent disk not found: diskName=%q", diskName)
	}
	return disk, err
}

// Scans all managed zones to return the GCE PD. Only non-regional PDs are searched.
// Prefer getDiskByName, if the zone can be established
// Return cloudprovider.DiskNotFound if the given disk cannot be found in any zone
func (gce *GCECloud) GetDiskByNameUnknownZone(diskName string) (*GCEDisk, error) {
	// TODO (verult) update this comment
	// Note: this is the gotcha right now with GCE PD support:
	// disk names are not unique per-region.
	// (I can create two volumes with name "myvol" in e.g. us-central1-b & us-central1-f)
	// For now, this is simply undefined behaviour.
	//
	// In future, we will have to require users to qualify their disk
	// "us-central1-a/mydisk".  We could do this for them as part of
	// admission control, but that might be a little weird (values changing
	// on create)

	var found *GCEDisk
	for _, zone := range gce.managedZones {
		disk, err := gce.findDiskByName(diskName, zone)
		if err != nil {
			return nil, err
		}
		// findDiskByName returns (nil,nil) if the disk doesn't exist, so we can't
		// assume that a disk was found unless disk is non-nil.
		if disk == nil {
			continue
		}
		if found != nil {
			// TODO (verult) Remove zoneinfo?
			// findDiskByName always returns a disk with zoneInfo 'singleZone'
			return nil, fmt.Errorf("GCE PD name was found in multiple zones: %q", diskName)
		}
		found = disk
	}

	if found != nil {
		return found, nil
	}
	glog.Warningf("GCE persistent disk %q not found in managed zones (%s)",
		diskName, strings.Join(gce.managedZones, ","))

	return nil, cloudprovider.DiskNotFound
}

// encodeDiskTags encodes requested volume tags into JSON string, as GCE does
// not support tags on GCE PDs and we use Description field as fallback.
func (gce *GCECloud) encodeDiskTags(tags map[string]string) (string, error) {
	if len(tags) == 0 {
		// No tags -> empty JSON
		return "", nil
	}

	enc, err := json.Marshal(tags)
	if err != nil {
		return "", err
	}
	return string(enc), nil
}

func (gce *GCECloud) doDeleteDisk(diskToDelete string) error {
	disk, err := gce.GetDiskByNameUnknownZone(diskToDelete)
	if err != nil {
		return err
	}

	// TODO (verult) Remove zoneinfo?
	// GetDiskByNameUnknownZone always returns a disk with zoneInfo 'singleZone'

	switch zoneInfo := disk.ZoneInfo.(type) {
	case singleZone:
		mc := newDiskMetricContextZonal("delete", disk.Region, zoneInfo.zone)
		return mc.Observe(gce.manager.DeleteDiskOnCloudProvider(zoneInfo.zone, disk.Name))
	case nil:
		// TODO (verult) is this possible?
		return fmt.Errorf("PD has nil ZoneInfo: %v", disk)
	default:
		return fmt.Errorf("disk.ZoneInfo has unexpected type %T", zoneInfo)
	}
}

// isGCEError returns true if given error is a googleapi.Error with given
// reason (e.g. "resourceInUseByAnotherResource")
func isGCEError(err error, reason string) bool {
	apiErr, ok := err.(*googleapi.Error)
	if !ok {
		return false
	}

	for _, e := range apiErr.Errors {
		if e.Reason == reason {
			return true
		}
	}
	return false
}
