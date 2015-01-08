import asyncio
import os
import subprocess

import pyinotify


RSYNC = "rsync"

class EventHandler:
    '''
    Handles file events.
    '''
    def __init__(self, source, dest, loop):
        '''
        @param source - source directory
        @param dest - destination directory
        @param loop - asyncio loop
        '''
        self._source = source
        self._dest = dest
        self._loop = loop

    def __call__(self, event):
        '''
        A wrapper for the synchronize coroutine.

        @param event - write, read, create etc event.
        '''
        asyncio.async(self._synchronize(event))

    @asyncio.coroutine
    def _synchronize(self, event):
        '''
        This method invokes rsync and synchronizes destination with the source folder.

        @param event - write, read, create etc event.
        '''
        create = asyncio.create_subprocess_exec(RSYNC, '--links', '-av', '--files-from=-', self._source, self._dest,
                                                stdin=asyncio.subprocess.PIPE,
                                                stdout=asyncio.subprocess.PIPE)
        proc = yield from create
        print("Got event:", event)
        filename = os.path.basename(event.pathname)

        proc.stdin.write(filename.encode())
        yield from proc.stdin.drain()
        proc.stdin.write_eof()
        print("=========DRAINED===========")

        data = yield from proc.stdout.readline()
        line = data.decode('ascii').rstrip()

        yield from proc.wait()
        print("Got response", line)


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

    def add_local_monitor(self, source, dest, events):
        '''
        Adds a monitor to watch source. Destination gets updated when source
        changes. Source and dest must be directories. Both source and
        destination must be located on the same host.

        @param source - source directory
        @param dest - destination directory
        '''
        if not (os.path.isdir(source) & os.path.isdir(dest)):
            raise ValueError("Source, and destination parameters must be folders.")

        self._wm.add_watch(source,
                           events,
                           proc_fun=EventHandler(source, dest, self._loop))


def main():
    loop = asyncio.get_event_loop()
    auto_sync = AutoSync(loop)
    auto_sync.add_local_monitor("/tmp/source",
                                "/tmp/dest",
                                pyinotify.IN_CREATE | pyinotify.IN_MODIFY)

    try:
        loop.run_forever()
    except (SystemExit, KeyboardInterrupt):
        print("Exiting...")

if __name__ == "__main__":
    main()

