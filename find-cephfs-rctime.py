#!/usr/bin/env python
import argparse
import os
import sys

from pprint import pprint
from multiprocessing import Process, JoinableQueue, Manager
from multiprocessing.managers import ListProxy
from itertools import islice


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
        if matches:
            pprint(matches)
            ctime_matches.append(matches)

        need_ctime_checked.task_done()


def main():
    parser = argparse.ArgumentParser(
            description="",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('root_paths', metavar='PATH', nargs='+')
    parser.add_argument('--threads', type=int)
    parser.add_argument('--min-ctime', type=float)
    args = parser.parse_args()
    pprint(args)

    need_ctime_checked = JoinableQueue()
    need_ctime_checked.put(args.root_paths)
    need_rctime_checked = JoinableQueue()
    need_rctime_checked.put(args.root_paths)

    rctime_checkers = [Process(target=rctime_checker,
                               args=(args.min_ctime, need_rctime_checked, need_ctime_checked,
                                     100))
                       for _ in range(args.threads)]

    ctime_matches = Manager().list()
    ctime_checkers = [Process(target=ctime_checker,
                              args=(args.min_ctime, need_ctime_checked, ctime_matches))
                      for _ in range(args.threads)]

    [p.start() for p in rctime_checkers]
    [p.start() for p in ctime_checkers]

    need_rctime_checked.join()
    [need_rctime_checked.put(None) for _ in range(args.threads)]

    need_ctime_checked.join()
    [need_ctime_checked.put(None) for _ in range(args.threads)]

    #need_rctime_checked.join()
    #need_ctime_checked.join()

    [p.join() for p in rctime_checkers]
    [p.join() for p in ctime_checkers]

    #while not ctime_matches.empty():
    #    bunch = ctime_matches.get()
    #    pprint(bunch)
    x = list(ctime_matches)
    #pprint(x)


if __name__ == '__main__':
    sys.exit(main())

