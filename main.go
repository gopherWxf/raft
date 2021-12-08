package main

import "raft/raft"

func main() {
	nodePool := map[string]string{
		"A": ":6870",
		"B": ":6871",
		"C": ":6872",
	}
	raft.Start(3, nodePool)
}
