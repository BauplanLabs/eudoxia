# Conversion constants to switch between seconds and microseconds. 
# 10 microseconds is the length of one tick
SEC_TO_MICROSECONDS = 1_000_000
MICROSEC_TO_SEC = float(1 / 1_000_000)
TICK_LENGTH_SECS = 10 * MICROSEC_TO_SEC

# 20GB/s scan NVMe
DISK_SCAN_GB_SEC = 20 
