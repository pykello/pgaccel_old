# SF100 direct-memory sweep and Q19 follow-up

Fixed-build sweep retained, including its Q19 failure. Q19 has a separately identified corrected-build follow-up. Nine native Python calls per successful query, each in a fresh 8 GiB zero-swap cgroup. Warm median excludes first preparation. Peers are saved measurements, not reruns; this is not a query-transition test.

The fixed build passed 21 queries and was OOM-killed on Q19. Its follow-up uses a newer preparation rule for literals shared by every OR branch. All 198 successful trials pass the typed oracle and verify live memory limits. The 768 MiB query allowance stays inside the 8 GiB total cap.

| Query | Previous pgaccel ms | Direct warm ms | ClickHouse ms (saved) | DuckDB ms (saved) | Peak cgroup GiB | First call s |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 440.185 | 427.851 | 6649.361 | 2403.602 | 6.884 | 28.648 |
| Q2 | 77.479 | 87.405 | 1345.272 | 337.937 | 1.331 | 3.292 |
| Q3 | 430.604 | 344.928 | 3939.540 | 1976.796 | 2.511 | 16.619 |
| Q4 | 209.989 | 184.016 | 2949.705 | 1685.489 | 4.535 | 15.743 |
| Q5 | 374.163 | 303.455 | 4842.277 | 1997.345 | 2.960 | 19.775 |
| Q6 | 79.652 | 4.592 | 1816.369 | 513.654 | 0.157 | 9.598 |
| Q7 | 521.101 | 460.432 | 4598.745 | 2005.950 | 4.385 | 21.775 |
| Q8 | 658.360 | 394.974 | 5292.148 | 1998.868 | 2.195 | 21.428 |
| Q9 | 2174.809 | 1573.861 | 8905.443 | 6388.575 | 2.766 | 24.586 |
| Q10 | 1160.066 | 575.790 | 5006.751 | 3003.827 | 6.298 | 25.048 |
| Q11 | 30.496 | 43.219 | 941.140 | 323.997 | 1.114 | 2.347 |
| Q12 | 341.982 | 345.702 | 3434.824 | 1494.902 | 7.458 | 18.032 |
| Q13 | 1093.312 | 547.113 | 4769.721 | 5595.847 | 1.852 | 37.595 |
| Q14 | 132.674 | 117.373 | 2248.999 | 1124.596 | 0.209 | 11.555 |
| Q15 | 146.978 | 151.154 | 4184.006 | 829.427 | 6.533 | 14.595 |
| Q16 | 213.195 | 223.997 | 1378.175 | 1001.576 | 1.117 | 2.203 |
| Q17 | 272.281 | 272.799 | 4134.933 | 2563.513 | 5.762 | 12.774 |
| Q18 | 733.755 | 748.478 | 8271.662 | 4802.045 | 6.463 | 14.895 |
| Q19 (follow-up) | 217.095 | 96.720 | 4909.323 | 2547.487 | 2.057 | 17.272 |
| Q20 | 211.185 | 237.202 | 3173.749 | 1371.021 | 7.887 | 21.623 |
| Q21 | 682.195 | 690.349 | 13533.714 | 6558.207 | 6.844 | 19.733 |
| Q22 | 375.958 | 382.358 | 1147.882 | 1177.929 | 1.191 | 3.290 |

Warm medians above the previous pgaccel measurement: Q2, Q11, Q12, Q15, Q16, Q17, Q18, Q20, Q21, Q22. No new tolerance threshold is being used to declare completion.

Sum of latest warm medians: 8213.768 ms, previously 10577.516 ms. This arithmetic sum combines the identified builds and is not a measured workload-transition runtime.

The full objective remains unproven. First calls include substantial query-specific preparation, cache transitions and admission are unfinished, and regressions need investigation. Saved peer RSS values are retained in the JSON but exclude file cache and are not interchangeable with cgroup peaks.
