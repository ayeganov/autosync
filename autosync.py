import abc
import asyncio
import os
import subprocess

import pyinotify


RSYNC = "rsync"

class EventHandler(metaclass=abc.ABCMeta):
    '''
    Base class for handling file events. Defines the abstract methods for
    subclasses to handle specific cases of directory synchronization.
    '''

    def __call__(self, event):
        '''
        A wrapper for the synchronize coroutine.

        @param event - write, read, create etc event.
        '''
        asyncio.async(self._synchronize(event))


    @abc.abstractmethod
    def _synchronize(self, event):
        '''
        Method to be implemented by subclasses to handle destination specific
        synchronization.
        '''
        pass


class LocalSynchronizer(EventHandler):
    def __init__(self, sync_from, dest, loop):
        '''
        @param sync_from - sync_from directory
        @param dest - destination directory
        @param loop - asyncio loop
        '''
        self._sync_from = sync_from
        self._dest = dest
        self._loop = loop

    @asyncio.coroutine
    def _synchronize(self, event):
        '''
        This method invokes rsync and synchronizes destination with the sync_from folder.
        Both sync_from and destination directories must be local.

        @param event - write, read, create etc event.
        '''
        create = asyncio.create_subprocess_exec(RSYNC,
                                                '--links',
                                                '-av',
                                                '--files-from=-',
                                                self._sync_from, self._dest,
                                                stdin=asyncio.subprocess.PIPE,
                                                stdout=asyncio.subprocess.PIPE)
        proc = yield from create
        filename = os.path.basename(event.pathname)

        proc.stdin.write(filename.encode())
        yield from proc.stdin.drain()
        proc.stdin.write_eof()

        data = yield from proc.stdout.readline()
        line = data.decode('ascii').rstrip()

        yield from proc.wait()


class RemoteSynchronizer(EventHandler):
    def __init__(self, sync_from, sync_to_host, sync_to, loop):
        '''
        @param sync_from - sync from directory
        @param sync_to_host - name(ip) of the host of the sync to directory
        @param sync_to - sync to directory
        @param loop - asyncio loop
        '''
        self._sync_from = sync_from
        self._sync_to_host = sync_to_host
        self._sync_to = sync_to
        self._loop = loop

    @asyncio.coroutine
    def _synchronize(self, event):
        '''
        This method invokes rsync and synchronizes destination with the sync_from folder.
        Both sync_from and destination directories must be local.

        @param event - write, read, create etc event.
        '''
        full_path_sync_to = "{0}:{1}".format(self._sync_to_host, self._sync_to)
        create = asyncio.create_subprocess_exec(RSYNC,
                                                '--links',
                                                '-av',
                                                '--files-from=-',
                                                self._sync_from,
                                                full_path_sync_to,
                                                stdin=asyncio.subprocess.PIPE,
                                                stdout=asyncio.subprocess.PIPE)
        proc = yield from create
        filename = os.path.basename(event.pathname)

        proc.stdin.write(filename.encode())
        yield from proc.stdin.drain()
        proc.stdin.write_eof()

        data = yield from proc.stdout.readline()
        line = data.decode('ascii').rstrip()

        yield from proc.wait()


class AutoSync:
    '''
    Class responsible for monitoring directories.
    '''
    def __init__(self, loop):
        '''
        @param loop - asyncio loop
        '''
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._wm = pyinotify.WatchManager()
        self._notifier = pyinotify.AsyncioNotifier(self._wm, self._loop)

    def add_local_syncher(self, sync_from, dest, events):
        '''
        Adds a monitor to watch sync_from. Destination gets updated when sync_from
        changes. sync_from and dest must be directories. Both sync_from and
        destination must be located on the same host.

        @param sync_from - sync_from directory
        @param dest - destination directory
        '''
        if not (os.path.isdir(sync_from) & os.path.isdir(dest)):
            raise ValueError("Sync from, and sync to parameters must be folders.")

        self._wm.add_watch(sync_from,
                           events,
                           proc_fun=LocalSynchronizer(sync_from, dest, self._loop))

    def add_remote_syncher(self, sync_from, remote_host, dest, events):
        if not os.path.isdir(sync_from):
            raise ValueError("Sync from must be a directory.")

        self._wm.add_watch(sync_from,
                           events,
                           proc_fun=RemoteSynchronizer(sync_from,
                                                       remote_host,
                                                       dest,
                                                       self._loop))


def main():
    loop = asyncio.get_event_loop()
    auto_sync = AutoSync(loop)
#    auto_sync.add_local_syncher("/tmp/source",
#                                "/tmp/dest",
#                                pyinotify.IN_CREATE | pyinotify.IN_MODIFY)
    auto_sync.add_remote_syncher("/tmp/source",
                                 "192.168.1.9",
                                 "/home/sasha/test",
                                 pyinotify.IN_CREATE | pyinotify.IN_MODIFY)

    try:
        loop.run_forever()
    except (SystemExit, KeyboardInterrupt):
        print("Exiting...")

if __name__ == "__main__":
    main()

