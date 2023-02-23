#!/bin/bash
# socket buffers
sysctl -w net.core.rmem_max=167772160
sysctl -w net.core.rmem_default=167772160
sysctl -w net.core.wmem_max=167772160
sysctl -w net.core.wmem_default=167772160

# maximum amount of option memory buffers 
sysctl -w net.core.optmem_max=167772000

# maximum buffer-space allocatable
# measured in units of pages (4096 bytes)
sysctl -w net.ipv4.tcp_rmem='40960 873800 167772160'
sysctl -w net.ipv4.tcp_wmem='40960 655360 167772160'
sysctl -w net.ipv4.tcp_mem='167772160 167772160 167772160'

sysctl -w net.ipv4.udp_mem='167772160 167772160 167772160'
sysctl -w net.ipv4.udp_rmem_min=16777216
sysctl -w net.ipv4.udp_wmem_min=16777216


sysctl -w net.core.rmem_default=167772160
sysctl -w net.core.wmem_default=167772160

sysctl -w net.core.netdev_max_backlog=300000
sysctl -w net.ipv4.route.flush=1
