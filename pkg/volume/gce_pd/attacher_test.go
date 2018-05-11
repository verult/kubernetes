/*
Copyright 2016 The Kubernetes Authors.

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

package gce_pd

import (
	"errors"
	"fmt"
	"testing"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"

	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/types"
	kubeletapis "k8s.io/kubernetes/pkg/kubelet/apis"
	"strings"
	"k8s.io/kubernetes/pkg/cloudprovider/providers/gce"
	"path"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubelet/apis"
)

func TestGetDeviceName_Volume(t *testing.T) {
	plugin := newPlugin()
	name := "my-pd-volume"
	spec := createVolSpec(name, false)

	deviceName, err := plugin.GetVolumeName(spec)
	if err != nil {
		t.Errorf("GetDeviceName error: %v", err)
	}
	if deviceName != name {
		t.Errorf("GetDeviceName error: expected %s, got %s", name, deviceName)
	}
}

func TestGetDeviceName_PersistentVolume(t *testing.T) {
	plugin := newPlugin()
	name := "my-pd-pv"
	spec := createPVSpec(name, true, nil)

	deviceName, err := plugin.GetVolumeName(spec)
	if err != nil {
		t.Errorf("GetDeviceName error: %v", err)
	}
	if deviceName != name {
		t.Errorf("GetDeviceName error: expected %s, got %s", name, deviceName)
	}
}

func TestGetDeviceName_PersistentVolumeRegional(t *testing.T) {
	plugin := newPlugin()
	name := "my-pd-pv"
	zones := []string {"zone1", "zone2"}
	spec := createPVSpec(name, true, zones)

	deviceName, err := plugin.GetVolumeName(spec)
	if err != nil {
		t.Errorf("GetDeviceName error: %v", err)
	}
	if deviceName != name + volNameRegionalSuffix {
		t.Errorf("GetDeviceName error: expected %s, got %s", name, deviceName)
	}
}

// One testcase for TestAttachDetach table test below
type testcase struct {
	name string
	// For fake GCE:
	attach         attachCall
	detach         detachCall
	diskIsAttached diskIsAttachedCall
	t              *testing.T

	// Actual test to run
	test func(test *testcase) error
	// Expected return of the test
	expectedReturn error
}

func TestAttachDetachRegional(t *testing.T) {
	diskName := "disk"
	nodeName := types.NodeName("instance")
	deviceName := diskName + volNameRegionalSuffix
	readOnly := false
	regional := true
	spec := createPVSpec(diskName, readOnly, []string{"zone1", "zone2"})
	// Successful Attach call
	testcase := testcase{
		name:           "Attach_Regional_Positive",
		diskIsAttached: diskIsAttachedCall{deviceName, nodeName, false, nil},
		attach:         attachCall{diskName, nodeName, deviceName, readOnly, regional, nil},
		test: func(testcase *testcase) error {
			attacher := newAttacher(testcase)
			devicePath, err := attacher.Attach(spec, nodeName)
			expectedPath := "/dev/disk/by-id/google-" + deviceName
			if devicePath != expectedPath {
				return fmt.Errorf("devicePath incorrect. Expected<%q> Actual: <%q>", expectedPath, devicePath)
			}
			return err
		},
	}

	err := testcase.test(&testcase)
	if err != nil && testcase.expectedReturn == nil {
		t.Errorf("%s failed: expected no errors, got %q", testcase.name, err.Error())
	} else if err != testcase.expectedReturn {
		t.Errorf("%s failed: expected err=%q, got %q", testcase.name, testcase.expectedReturn.Error(), err.Error())
	}
	t.Logf("Test %q succeeded", testcase.name)
}

func TestAttachDetach(t *testing.T) {
	diskName := "disk"
	nodeName := types.NodeName("instance")
	deviceName := diskName
	readOnly := false
	regional := false
	spec := createVolSpec(diskName, readOnly)
	attachError := errors.New("Fake attach error")
	detachError := errors.New("Fake detach error")
	diskCheckError := errors.New("Fake DiskIsAttached error")
	tests := []testcase{
		// Successful Attach call
		{
			name:           "Attach_Positive",
			diskIsAttached: diskIsAttachedCall{deviceName, nodeName, false, nil},
			attach:         attachCall{diskName, nodeName, deviceName, readOnly, regional, nil},
			test: func(testcase *testcase) error {
				attacher := newAttacher(testcase)
				devicePath, err := attacher.Attach(spec, nodeName)
				if devicePath != "/dev/disk/by-id/google-disk" {
					return fmt.Errorf("devicePath incorrect. Expected<\"/dev/disk/by-id/google-disk\"> Actual: <%q>", devicePath)
				}
				return err
			},
		},

		// Disk is already attached
		{
			name:           "Attach_Positive_AlreadyAttached",
			diskIsAttached: diskIsAttachedCall{diskName, nodeName, true, nil},
			test: func(testcase *testcase) error {
				attacher := newAttacher(testcase)
				devicePath, err := attacher.Attach(spec, nodeName)
				if devicePath != "/dev/disk/by-id/google-disk" {
					return fmt.Errorf("devicePath incorrect. Expected<\"/dev/disk/by-id/google-disk\"> Actual: <%q>", devicePath)
				}
				return err
			},
		},

		// DiskIsAttached fails and Attach succeeds
		{
			name:           "Attach_Positive_CheckFails",
			diskIsAttached: diskIsAttachedCall{deviceName, nodeName, false, diskCheckError},
			attach:         attachCall{diskName, nodeName, deviceName, readOnly, regional, nil},
			test: func(testcase *testcase) error {
				attacher := newAttacher(testcase)
				devicePath, err := attacher.Attach(spec, nodeName)
				if devicePath != "/dev/disk/by-id/google-disk" {
					return fmt.Errorf("devicePath incorrect. Expected<\"/dev/disk/by-id/google-disk\"> Actual: <%q>", devicePath)
				}
				return err
			},
		},

		// Attach call fails
		{
			name:           "Attach_Negative",
			diskIsAttached: diskIsAttachedCall{deviceName, nodeName, false, diskCheckError},
			attach:         attachCall{diskName, nodeName, deviceName, readOnly, regional, attachError},
			test: func(testcase *testcase) error {
				attacher := newAttacher(testcase)
				devicePath, err := attacher.Attach(spec, nodeName)
				if devicePath != "" {
					return fmt.Errorf("devicePath incorrect. Expected<\"\"> Actual: <%q>", devicePath)
				}
				return err
			},
			expectedReturn: attachError,
		},

		// Detach succeeds
		{
			name:           "Detach_Positive",
			diskIsAttached: diskIsAttachedCall{deviceName, nodeName, true, nil},
			detach:         detachCall{deviceName, nodeName, nil},
			test: func(testcase *testcase) error {
				detacher := newDetacher(testcase)
				return detacher.Detach(deviceName, nodeName)
			},
		},

		// Disk is already detached
		{
			name:           "Detach_Positive_AlreadyDetached",
			diskIsAttached: diskIsAttachedCall{deviceName, nodeName, false, nil},
			test: func(testcase *testcase) error {
				detacher := newDetacher(testcase)
				return detacher.Detach(deviceName, nodeName)
			},
		},

		// Detach succeeds when DiskIsAttached fails
		{
			name:           "Detach_Positive_CheckFails",
			diskIsAttached: diskIsAttachedCall{deviceName, nodeName, false, diskCheckError},
			detach:         detachCall{deviceName, nodeName, nil},
			test: func(testcase *testcase) error {
				detacher := newDetacher(testcase)
				return detacher.Detach(deviceName, nodeName)
			},
		},

		// Detach fails
		{
			name:           "Detach_Negative",
			diskIsAttached: diskIsAttachedCall{deviceName, nodeName, false, diskCheckError},
			detach:         detachCall{deviceName, nodeName, detachError},
			test: func(testcase *testcase) error {
				detacher := newDetacher(testcase)
				return detacher.Detach(deviceName, nodeName)
			},
			expectedReturn: detachError,
		},
	}

	for _, testcase := range tests {
		testcase.t = t
		err := testcase.test(&testcase)
		if err != nil && testcase.expectedReturn == nil {
			t.Errorf("%s failed: expected no errors, got %q", testcase.name, err.Error())
		} else if err != testcase.expectedReturn {
			t.Errorf("%s failed: expected err=%q, got %q", testcase.name, testcase.expectedReturn.Error(), err.Error())
		}
		t.Logf("Test %q succeeded", testcase.name)
	}
}
//
//type getDeviceMountPathTestCase struct {
//	spec *volume.Spec
//	expectedPath string
//	expectedErr error
//}

// TODO (verult) test WaitForAttach, GetDeviceMountPath
func TestGetDeviceMountPath(t *testing.T) {
	tests := []testcase{
		{
			name: "GetDeviceMountPath_InlineVolume",
			test: func(testcase *testcase) error {
				attacher := newAttacher(testcase)
				spec := &volume.Spec{
					Volume: &v1.Volume{
						Name: "vol1",
						VolumeSource: v1.VolumeSource{
							GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
								PDName: "pd",
								FSType: "ext4",
							},
						},
					},
				}
				mntPath, err := attacher.GetDeviceMountPath(spec)

				if err != nil {
					return err
				}

				deviceName := path.Base(mntPath)
				expectedDeviceName := "pd"

				if deviceName != expectedDeviceName {
					return fmt.Errorf("expected device name: %q; got: %q", expectedDeviceName, deviceName)
				}

				return nil
			},
		},
		{
			name: "GetDeviceMountPath_PV",
			test: func(testcase *testcase) error {
				attacher := newAttacher(testcase)
				spec := &volume.Spec{
					PersistentVolume: &v1.PersistentVolume{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								apis.LabelZoneFailureDomain: "zone1",
							},
						},
						Spec: v1.PersistentVolumeSpec{
							PersistentVolumeSource: v1.PersistentVolumeSource{
								GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
									PDName:   "pd",
									ReadOnly: false,
								},
							},
						},
					}}
				mntPath, err := attacher.GetDeviceMountPath(spec)

				if err != nil {
					return err
				}

				deviceName := path.Base(mntPath)
				expectedDeviceName := "pd"

				if deviceName != expectedDeviceName {
					return fmt.Errorf("expected device name: %q; got: %q", expectedDeviceName, deviceName)
				}

				return nil
			},
		},
		{
			name: "GetDeviceMountPath_PVRegional",
			test: func(testcase *testcase) error {
				attacher := newAttacher(testcase)
				spec := &volume.Spec{
					PersistentVolume: &v1.PersistentVolume{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								apis.LabelZoneFailureDomain: "zone1__zone2",
							},
						},
						Spec: v1.PersistentVolumeSpec{
							PersistentVolumeSource: v1.PersistentVolumeSource{
								GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
									PDName:   "pd",
									ReadOnly: false,
								},
							},
						},
					},
				}
				mntPath, err := attacher.GetDeviceMountPath(spec)

				if err != nil {
					return err
				}

				deviceName := path.Base(mntPath)
				expectedDeviceName := "pd_regional"

				if deviceName != expectedDeviceName {
					return fmt.Errorf("expected device name: %q; got: %q", expectedDeviceName, deviceName)
				}

				return nil
			},
		},
	}

	for _, testcase := range tests {
		testcase.t = t
		err := testcase.test(&testcase)
		if err != nil && testcase.expectedReturn == nil {
			t.Errorf("%s failed: expected no errors, got %q", testcase.name, err.Error())
		} else if err != testcase.expectedReturn {
			t.Errorf("%s failed: expected err=%q, got %q", testcase.name, testcase.expectedReturn.Error(), err.Error())
		}
		t.Logf("Test %q succeeded", testcase.name)
	}
}

// newPlugin creates a new gcePersistentDiskPlugin with fake cloud, NewAttacher
// and NewDetacher won't work.
func newPlugin() *gcePersistentDiskPlugin {
	host := volumetest.NewFakeVolumeHost(
		"/tmp", /* rootDir */
		nil,    /* kubeClient */
		nil,    /* plugins */
	)
	plugins := ProbeVolumePlugins()
	plugin := plugins[0]
	plugin.Init(host)
	return plugin.(*gcePersistentDiskPlugin)
}

func newAttacher(testcase *testcase) *gcePersistentDiskAttacher {
	rootDir := "/tmp/gcepdAttacherTest"
	return &gcePersistentDiskAttacher{
		host: volumetest.NewFakeVolumeHost(rootDir, nil, nil),
		gceDisks: testcase,
	}
}

func newDetacher(testcase *testcase) *gcePersistentDiskDetacher {
	return &gcePersistentDiskDetacher{
		gceDisks: testcase,
	}
}

func createVolSpec(name string, readOnly bool) *volume.Spec {
	return &volume.Spec{
		Volume: &v1.Volume{
			VolumeSource: v1.VolumeSource{
				GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
					PDName:   name,
					ReadOnly: readOnly,
				},
			},
		},
	}
}

func createPVSpec(name string, readOnly bool, zones []string) *volume.Spec {
	spec := &volume.Spec{
		PersistentVolume: &v1.PersistentVolume{
			Spec: v1.PersistentVolumeSpec{
				PersistentVolumeSource: v1.PersistentVolumeSource{
					GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
						PDName:   name,
						ReadOnly: readOnly,
					},
				},
			},
		},
	}

	if zones != nil {
		zonesLabel := strings.Join(zones, kubeletapis.LabelMultiZoneDelimiter)
		spec.PersistentVolume.ObjectMeta.Labels = map[string]string{
			kubeletapis.LabelZoneFailureDomain: zonesLabel,
		}
	}

	return spec
}

// Fake GCE implementation

type attachCall struct {
	diskName string
	nodeName types.NodeName
	deviceName string
	readOnly bool
	regional bool
	ret      error
}

type detachCall struct {
	devicePath string
	nodeName   types.NodeName
	ret        error
}

type diskIsAttachedCall struct {
	deviceName string
	nodeName   types.NodeName
	isAttached bool
	ret        error
}

var _ gce.Disks = &testcase{}

func (testcase *testcase) AttachDisk(diskName string, nodeName types.NodeName, deviceName string, readOnly bool, regional bool) error {
	// TODO (verult) include deviceName check
	expected := &testcase.attach

	if expected.diskName == "" && expected.nodeName == "" {
		// testcase.attach looks uninitialized, test did not expect to call
		// AttachDisk
		testcase.t.Errorf("Unexpected AttachDisk call!")
		return errors.New("Unexpected AttachDisk call!")
	}

	if expected.diskName != diskName {
		testcase.t.Errorf("Unexpected AttachDisk call: expected diskName %s, got %s", expected.diskName, diskName)
		return errors.New("Unexpected AttachDisk call: wrong diskName")
	}

	if expected.nodeName != nodeName {
		testcase.t.Errorf("Unexpected AttachDisk call: expected nodeName %s, got %s", expected.nodeName, nodeName)
		return errors.New("Unexpected AttachDisk call: wrong nodeName")
	}

	if expected.deviceName!= deviceName {
		testcase.t.Errorf("Unexpected AttachDisk call: expected deviceName %s, got %s", expected.deviceName, deviceName)
		return errors.New("Unexpected AttachDisk call: wrong deviceName")
	}

	if expected.readOnly != readOnly {
		testcase.t.Errorf("Unexpected AttachDisk call: expected readOnly %v, got %v", expected.readOnly, readOnly)
		return errors.New("Unexpected AttachDisk call: wrong readOnly")
	}

	if expected.regional != regional {
		testcase.t.Errorf("Unexpected AttachDisk call: expected regional %v, got %v", expected.regional, regional)
		return errors.New("Unexpected AttachDisk call: wrong regional")
	}

	glog.V(4).Infof("AttachDisk call: %s, %s, %v, returning %v", diskName, nodeName, readOnly, expected.ret)

	return expected.ret
}

// TODO (verult) refactor devicePath to deviceName
func (testcase *testcase) DetachDisk(devicePath string, nodeName types.NodeName) error {
	expected := &testcase.detach

	if expected.devicePath == "" && expected.nodeName == "" {
		// testcase.detach looks uninitialized, test did not expect to call
		// DetachDisk
		testcase.t.Errorf("Unexpected DetachDisk call!")
		return errors.New("Unexpected DetachDisk call!")
	}

	if expected.devicePath != devicePath {
		testcase.t.Errorf("Unexpected DetachDisk call: expected devicePath %s, got %s", expected.devicePath, devicePath)
		return errors.New("Unexpected DetachDisk call: wrong diskName")
	}

	if expected.nodeName != nodeName {
		testcase.t.Errorf("Unexpected DetachDisk call: expected nodeName %s, got %s", expected.nodeName, nodeName)
		return errors.New("Unexpected DetachDisk call: wrong nodeName")
	}

	glog.V(4).Infof("DetachDisk call: %s, %s, returning %v", devicePath, nodeName, expected.ret)

	return expected.ret
}

func (testcase *testcase) DiskIsAttached(deviceName string, nodeName types.NodeName) (bool, error) {
	expected := &testcase.diskIsAttached

	if expected.deviceName == "" && expected.nodeName == "" {
		// testcase.diskIsAttached looks uninitialized, test did not expect to
		// call DiskIsAttached
		testcase.t.Errorf("Unexpected DiskIsAttached call!")
		return false, errors.New("Unexpected DiskIsAttached call!")
	}

	if expected.deviceName != deviceName {
		testcase.t.Errorf("Unexpected DiskIsAttached call: expected deviceName %s, got %s", expected.deviceName, deviceName)
		return false, errors.New("Unexpected DiskIsAttached call: wrong deviceName")
	}

	if expected.nodeName != nodeName {
		testcase.t.Errorf("Unexpected DiskIsAttached call: expected nodeName %s, got %s", expected.nodeName, nodeName)
		return false, errors.New("Unexpected DiskIsAttached call: wrong nodeName")
	}

	glog.V(4).Infof("DiskIsAttached call: %s, %s, returning %v, %v", deviceName, nodeName, expected.isAttached, expected.ret)

	return expected.isAttached, expected.ret
}

func (testcase *testcase) DisksAreAttached(deviceNames []string, nodeName types.NodeName) (map[string]bool, error) {
	return nil, errors.New("Not implemented")
}

func (testcase *testcase) CreateDisk(name string, diskType string, zone string, sizeGb int64, tags map[string]string) error {
	return errors.New("Not implemented")
}

func (testcase *testcase) CreateRegionalDisk(name string, diskType string, replicaZones sets.String, sizeGb int64, tags map[string]string) error {
	return errors.New("Not implemented")
}

func (testcase *testcase) DeleteDisk(diskToDelete string, zone string) error {
	return errors.New("Not implemented")
}

func (testcase *testcase) DeleteRegionalDisk(diskToDelete string) error {
	return errors.New("Not implemented")
}

func (testcase *testcase) GetAutoLabelsForPD(name string, zone string, regional *bool) (map[string]string, error) {
	return map[string]string{}, errors.New("Not implemented")
}

func (testcase *testcase) ResizeDisk(
	diskName string,
	zoneSet sets.String,
	oldSize resource.Quantity,
	newSize resource.Quantity) (resource.Quantity, error) {
	return oldSize, errors.New("Not implemented")
}
