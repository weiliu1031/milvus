// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"errors"
	"sync"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

var (
	ErrNodeAlreadyExistInRG        = errors.New("Node already exist in resource group")
	ErrNodeNotExistInRG            = errors.New("Node doesn't exist in resource group")
	ErrNodeAlreadyAssign           = errors.New("Node already assign to other resource group")
	ErrRGIsFull                    = errors.New("Resource group is full")
	ErrRGNotExist                  = errors.New("Resource group doesn't exist")
	ErrRGAlreadyExist              = errors.New("Resource group already exist")
	ErrNoAvaliableRG               = errors.New("No avaliable resource group")
	ErrRGAssignNodeFailed          = errors.New("failed to assign node to resource group")
	ErrRGUnAssignNodeFailed        = errors.New("failed to unassign node from resource group")
	ErrSaveResourceGroupToStore    = errors.New("failed to save resource group to store")
	ErrRecoverResourceGroupToStore = errors.New("failed to recover resource group to store")
	ErrNodeNotAssignToRG           = errors.New("node hasn't been assign to any resource group")
)

var DefaultResourceGroupName = "__default_resource_group"

type ResourceGroup struct {
	nodes    UniqueSet
	capacity int
}

func NewResourceGroup(capacity int, nodes ...int64) *ResourceGroup {
	rg := &ResourceGroup{
		nodes:    typeutil.NewUniqueSet(),
		capacity: capacity,
	}

	for _, node := range nodes {
		rg.assignNode(node)
	}

	return rg
}

// assign node to resource group
func (rg *ResourceGroup) assignNode(id int64) error {
	err := rg.handleNodeUp(id)

	if err != nil {
		rg.capacity += 1
	}

	return err
}

// unassign node from resource group
func (rg *ResourceGroup) unassignNode(id int64) error {
	err := rg.handleNodeDown(id)

	if err != nil {
		rg.capacity -= 1
	}

	return err
}

func (rg *ResourceGroup) handleNodeUp(id int64) error {
	if rg.isFull() {
		return ErrRGIsFull
	}

	if rg.containsNode(id) {
		return ErrNodeAlreadyExistInRG
	}

	rg.nodes.Insert(id)
	return nil
}

func (rg *ResourceGroup) handleNodeDown(id int64) error {
	if !rg.containsNode(id) {
		return ErrNodeNotExistInRG
	}

	rg.nodes.Remove(id)
	return nil
}

func (rg *ResourceGroup) isFull() bool {
	return rg.nodes.Len() == rg.capacity
}

func (rg *ResourceGroup) containsNode(id int64) bool {
	return rg.nodes.Contain(id)
}

func (rg *ResourceGroup) getNodes() []int64 {
	return rg.nodes.Collect()
}

func (rg *ResourceGroup) clear() {
	for k := range rg.nodes {
		delete(rg.nodes, k)
	}
	rg.capacity = 0
}

func (rg *ResourceGroup) getCapacity() int {
	return rg.capacity
}

type ResourceManager struct {
	groups  map[string]*ResourceGroup
	store   Store
	nodeMgr *session.NodeManager

	rwmutex sync.RWMutex
}

func NewResourceManager(store Store, nodeMgr *session.NodeManager) *ResourceManager {
	groupMap := make(map[string]*ResourceGroup)
	groupMap[DefaultResourceGroupName] = NewResourceGroup(1024)
	return &ResourceManager{
		groups:  groupMap,
		store:   store,
		nodeMgr: nodeMgr,
	}
}

func (rm *ResourceManager) AddResourceGroup(rgName string, nodes ...int64) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	if rm.groups[rgName] != nil {
		return ErrRGAlreadyExist
	}

	rm.groups[rgName] = NewResourceGroup(len(nodes), nodes...)
	return nil
}

func (rm *ResourceManager) RemoveResourceGroup(rgName string) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	if rm.groups[rgName] == nil {
		return ErrRGNotExist
	}

	delete(rm.groups, rgName)
	return nil
}

func (rm *ResourceManager) AssignNode(rgName string, node int64) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	if rm.groups[rgName] == nil {
		return ErrRGNotExist
	}

	if rm.checkNodeAssigned(node) {
		return ErrNodeAlreadyAssign
	}

	err := rm.groups[rgName].assignNode(node)
	if err != nil {
		return err
	}

	err = rm.updateResourceGroupInStore(rgName)
	if err != nil {
		// roll back
		rm.groups[rgName].unassignNode(node)
		return ErrRGAssignNodeFailed
	}

	return nil
}

func (rm *ResourceManager) updateResourceGroupInStore(rgName string) error {
	err := rm.store.SaveResourceGroup(rgName, &querypb.ResourceGroup{
		Name:     rgName,
		Capacity: int32(rm.groups[rgName].getCapacity()),
		Nodes:    rm.groups[rgName].getNodes(),
	})

	if err != nil {
		return ErrSaveResourceGroupToStore
	}

	return nil
}

func (rm *ResourceManager) checkNodeAssigned(node int64) bool {
	for _, group := range rm.groups {
		if group.containsNode(node) {
			return true
		}
	}

	return false
}

func (rm *ResourceManager) UnassignNode(rgName string, node int64) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	if rm.groups[rgName] == nil {
		return ErrRGNotExist
	}

	err := rm.groups[rgName].unassignNode(node)
	if err != nil {
		return err
	}

	err = rm.updateResourceGroupInStore(rgName)
	if err != nil {
		// roll back
		rm.groups[rgName].assignNode(node)
		return ErrRGUnAssignNodeFailed
	}

	return nil
}

func (rm *ResourceManager) GetNodes(rgName string) ([]int64, error) {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()
	if rm.groups[rgName] == nil {
		return nil, ErrRGNotExist
	}

	return rm.groups[rgName].getNodes(), nil
}

func (rm *ResourceManager) ContainsNode(rgName string, node int64) bool {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()
	if rm.groups[rgName] == nil {
		return false
	}

	return rm.groups[rgName].containsNode(node)
}

func (rm *ResourceManager) findResourceGroupByNode(node int64) (string, error) {
	for name, group := range rm.groups {
		if group.containsNode(node) {
			return name, nil
		}
	}

	return "", ErrNodeNotAssignToRG
}

func (rm *ResourceManager) HandleNodeUp(node int64) (string, error) {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	if len(rm.groups) == 0 {
		return "", ErrNoAvaliableRG
	}

	rgName, err := rm.findResourceGroupByNode(node)
	if err == nil {
		return rgName, ErrNodeAlreadyAssign
	}

	minRgName := DefaultResourceGroupName
	for name, group := range rm.groups {
		// if rg lack nodes, assign new node to it
		if ok := group.isFull(); !ok {
			group.handleNodeUp(node)
			return name, nil
		}
	}

	// if all group is full, then assign node to group with the least nodes
	rm.AssignNode(minRgName, node)
	return minRgName, nil
}

func (rm *ResourceManager) HandleNodeDown(node int64) (string, error) {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	rgName, err := rm.findResourceGroupByNode(node)
	if err != nil {
		return "", err
	}

	return rgName, rm.groups[rgName].handleNodeDown(node)
}

func (rm *ResourceManager) TransferNode(from string, to string, node int64) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	err := rm.UnassignNode(from, node)

	if err != nil {
		return rm.AssignNode(to, node)
	}

	return err
}

func (rm *ResourceManager) Recover() error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	rgs, err := rm.store.GetResourceGroups()
	if err != nil {
		return ErrRecoverResourceGroupToStore
	}

	for _, rg := range rgs {
		rm.groups[rg.GetName()] = NewResourceGroup(int(rg.GetCapacity()), rg.GetNodes()...)
	}

	return nil
}

// todo: make sure whether this happen in server or rm
func (rm *ResourceManager) CheckNodeStatus() {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	for _, rg := range rm.groups {
		for _, node := range rg.getNodes() {
			if rm.nodeMgr.Get(node) == nil {
				rg.handleNodeDown(node)
			}
		}
	}
}
