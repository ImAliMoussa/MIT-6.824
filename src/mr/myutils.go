package mr

import (
	"errors"
)

func (c *Coordinator) getNextMapInput() (string, error) {
	for key := range c.mState.readyFiles {
		return key, nil
	}
	return "", errors.New("No ready files")
}

func (c *Coordinator) getNextReduceTask() (int, error) {
	for key := range c.rState.readyTasks {
		return key, nil
	}
	return -1, errors.New("No ready reduce tasks")
}

func (c *Coordinator) getFileFromMapReadyMap() string {
	if len(c.mState.readyFiles) == 0 {
		panic("Trying to read from empty map of files")
	}
	key, err := c.getNextMapInput()
	if err != nil {
		panic(err)
	}
	delete(c.mState.readyFiles, key)
	c.mState.inProgressFiles[key] = true
	return key
}

func (c *Coordinator) getTaskFromReduceReadyMap() int {
	if len(c.rState.readyTasks) == 0 {
		panic("Trying to read from empty ready tasks list")
	}
	key, err := c.getNextReduceTask()
	if err != nil {
		panic(err)
	}
	delete(c.rState.readyTasks, key)
	c.rState.inProgressTasks[key] = true
	return key
}

func (c *Coordinator) getRemainingMaps() int {
	return len(c.mState.readyFiles) + len(c.mState.inProgressFiles)
}

func (c *Coordinator) getRemainingTasks() int {
	return len(c.rState.readyTasks) + len(c.rState.inProgressTasks)
}

func (c *Coordinator) addTaskBackToReady(reduceTask int) {
	delete(c.rState.inProgressTasks, reduceTask)
	c.rState.readyTasks[reduceTask] = true
}

func (c *Coordinator) addMapFileBackToReady(file string) {
	delete(c.mState.inProgressFiles, file)
	c.mState.readyFiles[file] = true
}

func (c *Coordinator) findPosOfFile(filename string) int {
	for idx, value := range c.mState.allFiles {
		if value == filename {
			return idx + 1
		}
	}
	panic("File not found in all files")
}
