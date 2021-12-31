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
	mapsCompleted := len(c.mState.allFiles) -
		len(c.mState.readyFiles) -
		len(c.mState.inProgressFiles)
	return len(c.mState.allFiles) - mapsCompleted
}

func (c *Coordinator) getRemainingTasks() int {
	tasksCompleted := c.rState.nReduce -
		len(c.rState.readyTasks) -
		len(c.rState.inProgressTasks)
	return c.rState.nReduce - tasksCompleted
}

func (c *Coordinator) addTaskBackToReady(reduceTask int) {
	delete(c.rState.inProgressTasks, reduceTask)
	c.rState.readyTasks[reduceTask] = true
}

func (c *Coordinator) addMapFileBackToReady(file string) {
	delete(c.mState.inProgressFiles, file)
	c.mState.readyFiles[file] = true
}
