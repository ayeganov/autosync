import abc
import asyncio
import os
import pickle
import re
import subprocess

import pyinotify

import lib.async_zmq

RSYNC = "rsync"


class FileFilter:
    '''
    This class reads the exclude file and collects all file extensions that
    should be ignored by the syncer. Execute an instance of this class to check
    if file passes this filter.
    '''
    def __init__(self, config_path):
        self._file_exts = self._read_config(config_path)
        print("Extensions to ignore: {0}".format(self._file_exts))

    def _read_config(self, config):
        '''
        Reads the config file.

        Only uses proper extensions. Proper extension will have the following
        form:
            1) Could have whitespace in the beginning
            2) Could start with a "*"
            3) Must have a "." either as start, or followed after "*"
            4) Must contain only word characters [a-zA-Z0-9_]

        @param config - path to config file
        @returns set with all file extensions
        '''
        with open(config, 'r') as f:
            lines = f.read()

        pattern = r"^\s*(\*)?(\.\w+)$"
        exts = (match[1] for match in re.findall(pattern, lines, re.MULTILINE))
        return set(exts)

    def __call__(self, file_path):
        '''
        Checks the given string against the file filter, and verfies whether
        this file passes the filter.

        @param file_path - string represenging file path
        @returns True if extension is part of the filter, False otherwise
        '''
        name, ext = os.path.splitext(file_path)
        if ext == "":
            return False
        else:
            return ext in self._file_exts


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
    def __init__(self, exclusions, sync_from, sync_to, loop):
        '''
        @param exclusions - file filter describing what files to skip from sync
        @param sync_from - sync_from directory
        @param sync_to - destination directory
        @param loop - asyncio loop
        '''
        self._exclusions = exclusions
        self._sync_from = sync_from
        self._sync_to = sync_to
        self._loop = loop

    @asyncio.coroutine
    def _synchronize(self, event):
        '''
        This method invokes rsync and synchronizes destination with the sync_from folder.
        Both sync_from and destination directories must be local.

        @param event - write, read, create etc event.
        '''
        filename = os.path.basename(event.pathname)
        if self._exclusions(filename):
            return

        create = asyncio.create_subprocess_exec(RSYNC,
                                                '--links',
                                                '-av',
                                                '--files-from=-',
                                                self._sync_from, self._sync_to,
                                                stdin=asyncio.subprocess.PIPE,
                                                stdout=asyncio.subprocess.PIPE)
        proc = yield from create

        proc.stdin.write(filename.encode())
        yield from proc.stdin.drain()
        proc.stdin.write_eof()

        data = yield from proc.stdout.readline()
        line = data.decode('ascii').rstrip()

        yield from proc.wait()


class RemoteSynchronizer(EventHandler):
    def __init__(self, exclusions, sync_from, sync_to_host, sync_to, loop):
        '''
        @param exclusions - file filter describing what files to skip from sync
        @param sync_from - sync from directory
        @param sync_to_host - name(ip) of the host of the sync to directory
        @param sync_to - sync to directory
        @param loop - asyncio loop
        '''
        self._exclusions = exclusions
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
        filename = os.path.basename(event.pathname)
        if self._exclusions(filename):
            print("Skipping file: {0}".format(filename))
            return

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
    CONFIG_PATH = "/etc/autosyncd/exclude"
    def __init__(self, controller, loop=None):
        '''
        @param controller - communications controller
        @param loop - asyncio loop
        '''
        self._controller = controller
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._wm = pyinotify.WatchManager()
        self._notifier = pyinotify.AsyncioNotifier(self._wm, self._loop)
        self._exclusions = FileFilter(AutoSync.CONFIG_PATH)

    def add_local_syncher(self, sync_from, sync_to, events):
        '''
        Adds a monitor to watch sync_from. Destination gets updated when sync_from
        changes. sync_from and dest must be directories. Both sync_from and
        destination must be located on the same host.

        @param sync_from - sync_from directory
        @param sync_to - destination directory
        @param events - set of events which trigger this syncher
        '''
        print("Creating local syncher.")
        if not (os.path.isdir(sync_from) & os.path.isdir(sync_to)):
            raise ValueError("Sync from, and sync to parameters must be folders.")

        self._wm.add_watch(sync_from,
                           events,
                           proc_fun=LocalSynchronizer(self._exclusions,
                                                      sync_from,
                                                      sync_to,
                                                      self._loop))

    def add_remote_syncher(self, sync_from, remote_host, sync_to, events):
        '''
        Adds a monitor to watch sync_from. sync_to gets updated when sync_from
        changes. sync_from and dest must be directories. sync_from must be
        local, and sync_to must be located on the remote_host.

        @param sync_from - sync_from directory
        @param remote_host - server on which sync_to is located
        @param sync_to - destination directory
        @param events - set of events which trigger this syncher
        '''
        print("Creating remote syncher.")
        if not os.path.isdir(sync_from):
            raise ValueError("Sync from must be a directory.")

        self._wm.add_watch(sync_from,
                           events,
                           proc_fun=RemoteSynchronizer(self._exclusions,
                                                       sync_from,
                                                       remote_host,
                                                       sync_to,
                                                       self._loop))

class AutoSyncController:
    '''
    This class wraps the AutoSync object and facilitates communication between
    AutoSync and the rest of the system via ZMQ.
    '''
    def __init__(self, loop=None):
        '''
        @param loop - asyncio loop
        '''
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._auto_sync = AutoSync(self, self._loop)
        self._sync_rep = lib.async_zmq.SocketFactory.rep_socket(topic="/tmp/add_syncher",
                                                                on_recv=self._handle_new_synch_path,
                                                                loop=self._loop)

    def _handle_new_synch_path(self, msgs):
        '''
        Handles messages for creating new sync paths.

        @param msgs - msgs containing information about sync directories, and
                      file events mask(controls what file system events would
                      trigger the sync)
        '''
        try:
            for msg in msgs:
                msg_dict = pickle.loads(msg)
                print("Got new path message:", msg_dict)
                sync_from = msg_dict['sync_from']
                sync_to = msg_dict['sync_to']
                events = msg_dict['events']

                if 'remote_host' in msg_dict:
                    remote_host = msg_dict['remote_host']
                    self._auto_sync.add_remote_syncher(sync_from,
                                                       remote_host,
                                                       sync_to,
                                                       events)
                else:
                    self._auto_sync.add_local_syncher(sync_from, sync_to, events)

            response = {"success" : True, "error" : None}
            self._sync_rep.send(pickle.dumps(response))

        except Exception as e:
            print(e)
            error_msg = {"success" : False, "error" : e}
            self._sync_rep.send(pickle.dumps(error_msg))


def main():
    loop = asyncio.get_event_loop()
    auto_sync_controller = AutoSyncController(loop)

#    auto_sync.add_local_syncher("/tmp/source",
#                                "/tmp/dest",
#                                pyinotify.IN_CREATE | pyinotify.IN_MODIFY)
#    auto_sync.add_remote_syncher("/tmp/source",
#                                 "192.168.1.9",
#                                 "/home/sasha/test",
#                                 pyinotify.IN_CREATE | pyinotify.IN_MODIFY)

    try:
        print("Awaiting new messages.")
        loop.run_forever()
    except (SystemExit, KeyboardInterrupt):
        print("Exiting...")

if __name__ == "__main__":
    main()

