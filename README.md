# Distributed System

- [x] Lab 1
- [x] Lab 2A
- [X] Lab 2B
- [X] Lab 2C
- [X] Lab 2D
- [X] Lab 3A

```shell
time go test
Test (2A): initial election ...
  ... Passed --   3.1  3  142   42176    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  324   66874    0
Test (2A): multiple elections ...
  ... Passed --   5.4  7 1375  282001    0
Test (2B): basic agreement ...
  ... Passed --   0.5  3   16    4712    3
Test (2B): RPC byte count ...
  ... Passed --   1.2  3   48  114996   11
Test (2B): test progressive failure of followers ...
  ... Passed --   4.4  3  376   81159    3
Test (2B): test failure of leaders ...
  ... Passed --   5.0  3  511  121301    3
Test (2B): agreement after follower reconnects ...
  ... Passed --   5.3  3  307   85101    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.4  5  524  107579    4
Test (2B): concurrent Start()s ...
  ... Passed --   0.6  3   20    5944    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   6.1  3  468  123272    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  14.9  5 4370 4259142  103
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.1  3   92   28154   12
Test (2C): basic persistence ...
  ... Passed --   3.1  3  133   37555    6
Test (2C): more persistence ...
  ... Passed --  14.6  5 2131  467705   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.3  3   48   13643    4
Test (2C): Figure 8 ...
  ... Passed --  24.7  5 1278  294578   26
Test (2C): unreliable agreement ...
  ... Passed --   2.5  5  443  143180  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  36.0  5 13220 33022470  310
Test (2C): churn ...
  ... Passed --  16.2  5 3190 1766282  434
Test (2C): unreliable churn ...
  ... Passed --  16.3  5 4582 3546613  124
Test (2D): snapshots basic ...
  ... Passed --   3.1  3  182   66270  218
Test (2D): install snapshots (disconnect) ...
  ... Passed --  36.7  3 2081  828264  328
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  40.1  3 2359  877976  309
Test (2D): install snapshots (crash) ...
  ... Passed --  26.9  3 1366  698518  326
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  30.2  3 1530  751519  353
Test (2D): crash and restart all servers ...
  ... Passed --   6.7  3  276   87552   62
Test (2D): snapshot initialization after crash ...
  ... Passed --   1.9  3   68   20610   14
PASS
ok      6.5840/raft     316.606s

real    5m16.793s
user    0m18.729s
sys     0m3.672s

```
