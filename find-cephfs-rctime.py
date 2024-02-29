#!/usr/bin/env python
import argparse
import os
import sys
from dateutil.parser import parse

from pprint import pprint
from pathlib import Path
from multiprocessing import Process, JoinableQueue, Manager
from multiprocessing.managers import ListProxy
from itertools import islice, chain


def batched(iterable, n):
    # batched('ABCDEFG', 3) --> ABC DEF G
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def rctime_checker(min_rctime: int, need_rctime_checked: JoinableQueue,
                   need_ctime_checked: JoinableQueue, max_chunk_size: int):
    while True:
        need_rctime_checked_buffer = []
        need_ctime_checked_buffer = []

        bunch = need_rctime_checked.get()

        if bunch is None:
            need_rctime_checked.task_done()
            return

        for dir_path in bunch:
            for entry in os.scandir(dir_path):
                need_ctime_checked_buffer.append(entry.path)

                if entry.is_dir(follow_symlinks=False):
                    rctime = os.getxattr(entry.path, "ceph.dir.rctime", follow_symlinks=False)
                    if min_rctime <= float(rctime):
                        need_rctime_checked_buffer.append(entry.path)

        for bunch in batched(need_rctime_checked_buffer, max_chunk_size):
            if bunch:
                need_rctime_checked.put(bunch)

        for bunch in batched(need_ctime_checked_buffer, max_chunk_size):
            if bunch:
                need_ctime_checked.put(bunch)

        need_rctime_checked.task_done()


def ctime_checker(min_ctime: int, need_ctime_checked: JoinableQueue, ctime_matches: ListProxy):
    while True:
        bunch = need_ctime_checked.get()

        if bunch is None:
            need_ctime_checked.task_done()
            return

        stats = [(path, os.stat(path, follow_symlinks=False)) for path in bunch]
        matches = [path for path, stat in stats if stat.st_ctime >= min_ctime]
        # isdir follows symlinks, so both it and islink can be true at the same time
        matches = [(p + '/' if os.path.isdir(p) and not os.path.islink(p) else p)
                   for p in matches]
        if matches:
            ctime_matches.append(matches)  # append is faster than extend

        need_ctime_checked.task_done()


def main():
    parser = argparse.ArgumentParser(
            description="",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('root_path', metavar='PATH', nargs=1,
                        help="Where to look for files")
    parser.add_argument('--threads', type=int, default=64,
                        help="number of threads to use")
    parser.add_argument('--min-ctime', metavar='DATE', required=True,
                        help="Minimum inclusive ctime as a date in a reasonable format")
    parser.add_argument('--dirs-only', action='store_true',
                        help="only print directories whose ctime is at least DATE "
                             "and directories that contain files whose ctime is "
                             "at least DATE")
    parser.add_argument('--relative', action='store_true',
                        help="print relative paths")
    args = parser.parse_args()

    root_path = args.root_path[0].rstrip('/')

    min_ctime = parse(args.min_ctime)
    min_ctime = min_ctime.timestamp()

    need_ctime_checked = JoinableQueue()
    need_ctime_checked.put([root_path])
    need_rctime_checked = JoinableQueue()
    need_rctime_checked.put([root_path])

    rctime_checkers = [Process(target=rctime_checker,
                               args=(min_ctime, need_rctime_checked, need_ctime_checked,
                                     100))
                       for _ in range(args.threads//2)]

    ctime_matches = Manager().list()
    ctime_checkers = [Process(target=ctime_checker,
                              args=(min_ctime, need_ctime_checked, ctime_matches))
                      for _ in range(args.threads//2)]

    [p.start() for p in rctime_checkers]
    [p.start() for p in ctime_checkers]

    need_rctime_checked.join()
    [need_rctime_checked.put(None) for _ in range(args.threads)]

    need_ctime_checked.join()
    [need_ctime_checked.put(None) for _ in range(args.threads)]

    [p.join() for p in rctime_checkers]
    [p.join() for p in ctime_checkers]

    results_iter = chain(*ctime_matches)
    if args.dirs_only:
        results = sorted(set((path if path.endswith('/') else os.path.dirname(path) + '/')
                         for path in results_iter))
    else:
        results = sorted(results_iter)
    if args.relative:
        results = [path.split(root_path + '/', maxsplit=1)[-1] for path in results]
    print('\n'.join(results))


if __name__ == '__main__':
    sys.exit(main())
