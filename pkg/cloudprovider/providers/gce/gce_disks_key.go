/*
Copyright 2018 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/util/sets"
	"strings"
)

// TODO (verult) this is really bad - this file contains knowledge between cloud provider and plugin.
//     Need better abstraction.
// TODO (verult) Should this be called a Key if it's many-to-one?

/*
 * In-line volume: only Name is set.
 * PV with regular PD: Name, Region, ZoneSet always set.
 * PV with regional PD: Name, Region always set; ZoneSet is set if constructed from spec,
 *     unset if constructed from device name.
 */
type DiskKey struct {
	Name string
	Region string
	ZoneSet sets.String
}

// TODO (verult) Create constructor that validates key before creation.
// If ZoneSet.Len() > 0, Region != ""

func (key *DiskKey) IsRegionalPD() bool {
	return key.Region != "" && key.ZoneSet.Len() != 1
}

func (key *DiskKey) IsZoneInfoAvailable() bool {
	return key.ZoneSet.Len() > 0
}

// "{<name>,<region>,<zone1__zone2>}"
func (key *DiskKey) String() string {
	str := "{" + key.Name

	if key.Region != "" {
		str += "," + key.Region

		if key.ZoneSet.Len() > 0 {
			str += "," + strings.Join(key.ZoneSet.List(), "__")
		}
	}

	str += "}"

	return str
}