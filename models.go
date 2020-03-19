package main

type cluster struct {
	Name            string
	CPUTotal        int64
	CPUReserved     int64
	CPUCoreTotal    int64
	CPUCoreReserved int64
	MemoryTotal     int64
	MemoryReserved  int64
	DiskTotal       int64
	DiskReserved    int64
	MemoryFactor    float64
	CPUFactor       float64
	CPUThreshold    float64
}
