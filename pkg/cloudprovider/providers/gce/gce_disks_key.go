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

import "strings"

const deviceNameSeparator = "_"

// TODO (verult) We need a better name than DiskInfo
type DiskInfo interface{
	GetDiskName() string
	GetDeviceName() string
}

type PartialDiskInfo struct {
	Name string
}

type RegionalDiskInfo struct {
	Name string
	Region string
}

type ZonalDiskInfo struct {
	Name string
	Region string
	Zone string
}

func (d PartialDiskInfo) GetDiskName() string { return d.Name }
func (d RegionalDiskInfo) GetDiskName() string { return d.Name }
func (d ZonalDiskInfo) GetDiskName() string { return d.Name }

func (d PartialDiskInfo) GetDeviceName() string {
	return d.Name
}

func (d RegionalDiskInfo) GetDeviceName() string {
	return d.Name + deviceNameSeparator + d.Region
}

func (d ZonalDiskInfo) GetDeviceName() string {
	return d.Name + deviceNameSeparator + d.Region + deviceNameSeparator + d.Zone
}

func DeviceNameToDiskInfo(deviceName string) DiskInfo {
	parts := strings.Split(deviceName, deviceNameSeparator)

	switch len(parts) {
	case 1:
		return PartialDiskInfo{
			Name: parts[0],
		}
	case 2:
		return RegionalDiskInfo{
			Name: parts[0],
			Region: parts[1],
		}
	case 3:
		return ZonalDiskInfo{
			Name: parts[0],
			Region: parts[1],
			Zone: parts[2],
		}
	default: // Should not happen given a valid device name
		return nil
	}
}

// TODO (verult) Stringer method

//
///*
// * In-line volume: only Name is set.
// * PV with regular PD: Name, Region, ZoneSet always set.
// * PV with regional PD: Name, Region always set; ZoneSet is set if constructed from spec,
// *     unset if constructed from device name.
// */
//type DiskInfo struct {
//	Name string
//	Region string
//	ZoneSet sets.String
//}
//
//// TODO (verult) Create constructor that validates key before creation.
//// If ZoneSet.Len() > 0, Region != ""
//
//func (key *DiskInfo) IsRegionalPD() bool {
//	return key.Region != "" && key.ZoneSet.Len() != 1
//}
//
//func (key *DiskInfo) IsZoneInfoAvailable() bool {
//	return key.ZoneSet.Len() > 0
//}
//
//// "{<name>,<region>,<zone1__zone2>}"
//func (key *DiskInfo) String() string {
//	str := "{" + key.Name
//
//	if key.Region != "" {
//		str += "," + key.Region
//
//		if key.ZoneSet.Len() > 0 {
//			str += "," + strings.Join(key.ZoneSet.List(), "__")
//		}
//	}
//
//	str += "}"
//
//	return str
//}