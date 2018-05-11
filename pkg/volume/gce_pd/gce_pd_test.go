/*
Copyright 2014 The Kubernetes Authors.

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
	"fmt"
	"os"
	"path"
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	utiltesting "k8s.io/client-go/util/testing"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/kubelet/apis"
)

func TestCanSupport(t *testing.T) {
	tmpDir, err := utiltesting.MkTmpdir("gcepdTest")
	if err != nil {
		t.Fatalf("can't make a temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, volumetest.NewFakeVolumeHost(tmpDir, nil, nil))

	plug, err := plugMgr.FindPluginByName("kubernetes.io/gce-pd")
	if err != nil {
		t.Errorf("Can't find the plugin by name")
	}
	if plug.GetPluginName() != "kubernetes.io/gce-pd" {
		t.Errorf("Wrong name: %s", plug.GetPluginName())
	}
	if !plug.CanSupport(&volume.Spec{Volume: &v1.Volume{VolumeSource: v1.VolumeSource{GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{}}}}) {
		t.Errorf("Expected true")
	}
	if !plug.CanSupport(&volume.Spec{PersistentVolume: &v1.PersistentVolume{Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{}}}}}) {
		t.Errorf("Expected true")
	}
}

func TestGetAccessModes(t *testing.T) {
	tmpDir, err := utiltesting.MkTmpdir("gcepdTest")
	if err != nil {
		t.Fatalf("can't make a temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, volumetest.NewFakeVolumeHost(tmpDir, nil, nil))

	plug, err := plugMgr.FindPersistentPluginByName("kubernetes.io/gce-pd")
	if err != nil {
		t.Errorf("Can't find the plugin by name")
	}
	if !volumetest.ContainsAccessMode(plug.GetAccessModes(), v1.ReadWriteOnce) || !volumetest.ContainsAccessMode(plug.GetAccessModes(), v1.ReadOnlyMany) {
		t.Errorf("Expected two AccessModeTypes:  %s and %s", v1.ReadWriteOnce, v1.ReadOnlyMany)
	}
}

type fakePDManager struct {
}

// TODO (verult) Create/DeleteVolume logic not tested?
func (fake *fakePDManager) CreateVolume(c *gcePersistentDiskProvisioner) (volumeID string, volumeSizeGB int, labels map[string]string, fstype string, err error) {
	labels = make(map[string]string)
	labels["fakepdmanager"] = "yes"
	return "test-gce-volume-name", 100, labels, "", nil
}

func (fake *fakePDManager) DeleteVolume(cd *gcePersistentDiskDeleter) error {
	if cd.pdName != "test-gce-volume-name" {
		return fmt.Errorf("Deleter got unexpected volume name: %s", cd.pdName)
	}
	return nil
}

type deviceNameTestCase struct {
	spec *volume.Spec
	deviceName string
}

// TODO (verult) break into separate tests?
// TODO (verult) test resize?
func TestDeviceName(t *testing.T) {
	plug, teardownFunc, _ := getPDPlugin(t, nil)
	defer teardownFunc()

	tests := []deviceNameTestCase{
		{
			spec: &volume.Spec{
				Volume: &v1.Volume{
					Name: "vol1",
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "pd",
							FSType: "ext4",
						},
					},
				},
			},
			deviceName: "pd",
		},
		{
			spec: &volume.Spec{
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
			}},
			deviceName: "pd",
		},
		{
			spec: &volume.Spec{
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
			},
			deviceName: "pd_regional",
		},
	}
	fakeManager := &fakePDManager{}
	fakeMounter := &mount.FakeMounter{}

	for _, test := range tests {
		mounter, err := plug.(*gcePersistentDiskPlugin).newMounterInternal(test.spec, types.UID("poduid"), fakeManager, fakeMounter)
		if err != nil {
			t.Errorf("Failed to make a new Mounter: %v", err)
		}
		if mounter == nil {
			t.Errorf("Got a nil Mounter")
		}

		if err := mounter.SetUp(nil); err != nil {
			t.Errorf("Expected success, got: %v", err)
		}

		l := len(fakeMounter.MountPoints)
		if l != 1 {
			t.Errorf("Expected 1 mount point, got: %d", l)
		}

		mp := fakeMounter.MountPoints[0]
		if path.Base(mp.Device) != test.deviceName { // TODO (verult) make this independent of spec
			t.Errorf("Expected device path to end with %v, got: %v", test.deviceName, path.Base(mp.Device))
		}
		fakeMounter.MountPoints = nil
	}
}

func TestPlugin(t *testing.T) {
	plug, teardownFunc, rootDir := getPDPlugin(t, nil)
	defer teardownFunc()

	spec := &v1.Volume{
		Name: "vol1",
		VolumeSource: v1.VolumeSource{
			GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
				PDName: "pd",
				FSType: "ext4",
			},
		},
	}
	fakeManager := &fakePDManager{}
	fakeMounter := &mount.FakeMounter{}
	mounter, err := plug.(*gcePersistentDiskPlugin).newMounterInternal(volume.NewSpecFromVolume(spec), types.UID("poduid"), fakeManager, fakeMounter)
	if err != nil {
		t.Errorf("Failed to make a new Mounter: %v", err)
	}
	if mounter == nil {
		t.Errorf("Got a nil Mounter")
	}

	volPath := path.Join(rootDir, "pods/poduid/volumes/kubernetes.io~gce-pd/vol1")
	path := mounter.GetPath()
	if path != volPath {
		t.Errorf("Got unexpected path: %s", path)
	}

	if err := mounter.SetUp(nil); err != nil {
		t.Errorf("Expected success, got: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			t.Errorf("SetUp() failed, volume path not created: %s", path)
		} else {
			t.Errorf("SetUp() failed: %v", err)
		}
	}

	fakeManager = &fakePDManager{}
	unmounter, err := plug.(*gcePersistentDiskPlugin).newUnmounterInternal("vol1", types.UID("poduid"), fakeManager, fakeMounter)
	if err != nil {
		t.Errorf("Failed to make a new Unmounter: %v", err)
	}
	if unmounter == nil {
		t.Errorf("Got a nil Unmounter")
	}

	if err := unmounter.TearDown(); err != nil {
		t.Errorf("Expected success, got: %v", err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Errorf("TearDown() failed, volume path still exists: %s", path)
	} else if !os.IsNotExist(err) {
		t.Errorf("TearDown() failed: %v", err)
	}

	// Test Provisioner
	options := volume.VolumeOptions{
		PVC: volumetest.CreateTestPVC("100Mi", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}),
		PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
	}
	provisioner, err := plug.(*gcePersistentDiskPlugin).newProvisionerInternal(options, &fakePDManager{})
	if err != nil {
		t.Errorf("Error creating new provisioner:%v", err)
	}
	persistentSpec, err := provisioner.Provision(nil, nil)
	if err != nil {
		t.Errorf("Provision() failed: %v", err)
	}

	if persistentSpec.Spec.PersistentVolumeSource.GCEPersistentDisk.PDName != "test-gce-volume-name" {
		t.Errorf("Provision() returned unexpected volume ID: %s", persistentSpec.Spec.PersistentVolumeSource.GCEPersistentDisk.PDName)
	}
	cap := persistentSpec.Spec.Capacity[v1.ResourceStorage]
	size := cap.Value()
	if size != 100*util.GB {
		t.Errorf("Provision() returned unexpected volume size: %v", size)
	}

	if persistentSpec.Labels["fakepdmanager"] != "yes" {
		t.Errorf("Provision() returned unexpected labels: %v", persistentSpec.Labels)
	}

	// Test Deleter
	volSpec := &volume.Spec{
		PersistentVolume: persistentSpec,
	}
	deleter, err := plug.(*gcePersistentDiskPlugin).newDeleterInternal(volSpec, &fakePDManager{})
	if err != nil {
		t.Errorf("Error creating new deleter:%v", err)
	}
	err = deleter.Delete()
	if err != nil {
		t.Errorf("Deleter() failed: %v", err)
	}
}

func TestPersistentClaimReadOnlyFlag(t *testing.T) {
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvA",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{},
			},
			ClaimRef: &v1.ObjectReference{
				Name: "claimA",
			},
		},
	}

	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claimA",
			Namespace: "nsA",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "pvA",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}

	client := fake.NewSimpleClientset(pv, claim)
	plug, teardownFunc, _ := getPDPlugin(t, client)
	defer teardownFunc()

	// readOnly bool is supplied by persistent-claim volume source when its mounter creates other volumes
	spec := volume.NewSpecFromPersistentVolume(pv, true)
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID("poduid")}}
	mounter, _ := plug.NewMounter(spec, pod, volume.VolumeOptions{})
	if mounter == nil {
		t.Fatalf("Got a nil Mounter")
	}

	if !mounter.GetAttributes().ReadOnly {
		t.Errorf("Expected true for mounter.IsReadOnly")
	}
}

func getPDPlugin(t *testing.T, client clientset.Interface) (plugin volume.VolumePlugin, teardown func(), rootDir string){
	tmpDir, err := utiltesting.MkTmpdir("gcepdTest")
	if err != nil {
		t.Fatalf("can't make a temp dir: %v", err)
	}
	plugMgr := volume.VolumePluginMgr{}
	plugMgr.InitPlugins(ProbeVolumePlugins(), nil /* prober */, volumetest.NewFakeVolumeHost(tmpDir, client, nil))
	plug, _ := plugMgr.FindPluginByName(gcePersistentDiskPluginName)

	return plug, func() { os.RemoveAll(tmpDir) }, tmpDir
}