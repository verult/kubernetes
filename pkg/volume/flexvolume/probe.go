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

package flexvolume

import (
	"io/ioutil"

	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"github.com/golang/glog"
)


type flexVolumeMetaPlugin struct {
	pluginDir string
}

func (*flexVolumeMetaPlugin) Init(host volume.VolumeHost) error {
	panic("implement me")
}

func (*flexVolumeMetaPlugin) GetPluginName() string {
	panic("implement me")
}

func (*flexVolumeMetaPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	panic("implement me")
}

func (*flexVolumeMetaPlugin) CanSupport(spec *volume.Spec) bool {
	panic("implement me")
}

func (*flexVolumeMetaPlugin) RequiresRemount() bool {
	panic("implement me")
}

func (*flexVolumeMetaPlugin) NewMounter(spec *volume.Spec, podRef *v1.Pod, opts volume.VolumeOptions) (volume.Mounter, error) {
	panic("implement me")
}

func (*flexVolumeMetaPlugin) NewUnmounter(name string, podUID types.UID) (volume.Unmounter, error) {
	panic("implement me")
}

func (*flexVolumeMetaPlugin) ConstructVolumeSpec(volumeName, mountPath string) (*volume.Spec, error) {
	panic("implement me")
}

func (*flexVolumeMetaPlugin) SupportsMountOption() bool {
	panic("implement me")
}

func (*flexVolumeMetaPlugin) SupportsBulkVolumeVerification() bool {
	panic("implement me")
}

//func (plugin *flexVolumeMetaPlugin) Init(host volume.VolumeHost) error {
//	// TODO implement
//	return nil
//}

func (plugin *flexVolumeMetaPlugin) Probe() []volume.VolumePlugin {
	plugins := []volume.VolumePlugin{}

	glog.Info("Probing Flexvolume directory...")

	files, _ := ioutil.ReadDir(plugin.pluginDir)
	for _, f := range files {
		// only directories are counted as plugins
		// and pluginDir/dirname/dirname should be an executable
		// unless dirname contains '~' for escaping namespace
		// e.g. dirname = vendor~cifs
		// then, executable will be pluginDir/dirname/cifs
		if f.IsDir() {
			plugin, err := NewFlexVolumePlugin(plugin.pluginDir, f.Name())
			if err != nil {
				continue
			}

			plugins = append(plugins, plugin)
		}
	}
	return plugins
}



// This is the primary entrypoint for volume plugins.
func ProbeVolumePlugins(pluginDir string) []volume.VolumePlugin {
	return []volume.VolumePlugin {
		&flexVolumeMetaPlugin{pluginDir: pluginDir},
	}
}
