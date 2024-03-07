This script uses cephfs's ceph.dir.rctime extended attribute to find files and directories whose ctime is on or after the supplied date.

The script is very fast (for python) because it's multithreaded (uses multiprocessing) and ceph.dir.rctime allows it to quickly zero-in on files of interest.

```
$ ./find-cephfs-rctime.py -h
usage: find-cephfs-rctime.py [-h] --min-ctime DATE [--relative] [--parents] [--threads NUM] PATH

Use cephfs's ceph.dir.rctime extended attribute to find files and directories whose ctime is on or after the
supplied date.

positional arguments:
  PATH              Where to look for files

options:
  -h, --help        show this help message and exit
  --min-ctime DATE  Minimum ctime in a reasonable format¹ (default: None)
  --relative        print matching paths relative to PATH (default: False)
  --parents         print only parent directories of matches (default: False)
  --threads NUM     number of threads to use² (default: 64)

Notes: (1) A variety of date formats can be parsed. YYYY-MM-DD is just one example. To specify Unix time,
prepend the number of seconds with @. (2) Since this script is IO-bound, it makes sense to use many more
threads than the number of CPUs.
```
