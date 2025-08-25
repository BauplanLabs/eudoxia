import pytest
from eudoxia.workload.pipeline import Segment
from eudoxia.utils import EudoxiaException, DISK_SCAN_GB_SEC


def test_segment_with_string_scaling():
    """Test segment creation with string-based scaling functions"""
    seg = Segment(
        baseline_cpu_seconds=5,
        cpu_scaling="linear3",
        storage_read_gb=10
    )
    
    # Test that scaling function works correctly
    scaled_time = seg.get_cpu_time(num_cpus=2)
    expected_time = 5 / 2  # linear3 scaling with 2 CPUs
    assert scaled_time == expected_time


def test_segment_with_callable_scaling():
    """Test segment creation with callable scaling function"""
    def custom_scaling(num_cpus, baseline_cpu_seconds):
        return baseline_cpu_seconds / num_cpus
    
    seg = Segment(
        baseline_cpu_seconds=10,
        cpu_scaling=custom_scaling,
        storage_read_gb=20
    )
    
    # Test that custom scaling function works correctly
    scaled_time = seg.get_cpu_time(num_cpus=4)
    expected_time = 10 / 4
    assert scaled_time == expected_time


def test_segment_io_calculations():
    """Test segment IO time calculations"""
    seg = Segment(
        baseline_cpu_seconds=2,
        cpu_scaling="const",
        storage_read_gb=50
    )
    
    # Test IO time calculation
    io_time_secs = seg.get_io_seconds()
    assert io_time_secs == 50 / DISK_SCAN_GB_SEC


def test_segment_memory_functionality():
    """Test segment memory functionality and defaults"""
    # Test default: memory_gb defaults to storage_read_gb
    seg1 = Segment(
        baseline_cpu_seconds=5,
        cpu_scaling="const", 
        storage_read_gb=25
    )
    assert seg1.get_peak_memory_gb() == 25
    
    # Test explicit memory_gb
    seg2 = Segment(
        baseline_cpu_seconds=5,
        cpu_scaling="const",
        memory_gb=10,
        storage_read_gb=25
    )
    assert seg2.get_peak_memory_gb() == 10


def test_segment_oom_functionality():
    """Test segment OOM detection functionality"""
    # Test fixed memory pattern - no OOM
    seg_fixed_ok = Segment(
        baseline_cpu_seconds=5,
        cpu_scaling="const",
        memory_gb=8,
        storage_read_gb=20
    )
    assert seg_fixed_ok.get_seconds_until_oom(10) is None  # 8 GB < 10 GB limit
    
    # Test fixed memory pattern - immediate OOM
    seg_fixed_oom = Segment(
        baseline_cpu_seconds=5,
        cpu_scaling="const",
        memory_gb=15,
        storage_read_gb=20
    )
    assert seg_fixed_oom.get_seconds_until_oom(10) == 0.0  # 15 GB > 10 GB limit
    
    # Test linear growth pattern - no OOM
    seg_linear_ok = Segment(
        baseline_cpu_seconds=5,
        cpu_scaling="const",
        storage_read_gb=8  # memory_gb=None, so peak = 8 GB
    )
    assert seg_linear_ok.get_seconds_until_oom(10) is None  # 8 GB < 10 GB limit
    
    # Test linear growth pattern - OOM at calculated time
    seg_linear_oom = Segment(
        baseline_cpu_seconds=5,
        cpu_scaling="const",
        storage_read_gb=20  # memory_gb=None, so peak = 20 GB
    )
    oom_time = seg_linear_oom.get_seconds_until_oom(10)  # 10 GB limit
    assert oom_time is not None
    
    # Verify the calculation: oom_time = limit_gb / DISK_SCAN_GB_SEC
    expected_oom_time = 10 / DISK_SCAN_GB_SEC  # limit / scan rate
    assert abs(oom_time - expected_oom_time) < 0.001


