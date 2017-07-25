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

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/volume"
)

type FlexVolumeProber interface {
	Probe() []volume.VolumePlugin
}

type flexVolumeProber struct {
	pluginDir string
}

func NewFlexVolumeProber(pluginDir string) FlexVolumeProber {
	return &flexVolumeProber{pluginDir: pluginDir}
}

func (plugin *flexVolumeProber) Probe() []volume.VolumePlugin {
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
	// TODO fix
	return []volume.VolumePlugin{}
}
