import abc
import asyncio
import functools
import grp
#import lockfile
import logging
import os
import pickle
import re
import signal
import sys

import daemon
import pyinotify

import lib.async_zmq

RSYNC = "rsync"

log = logging.getLogger("AutoSyncD")

def set_up_logging(config=None, log_location=None):
    """
    Configure default logging options.
    @param config - a ConfigParser object used to configure the logger objects
    """
    # Remove any handlers before calling basicConfig() as it's a no-op otherwise
    root_logger = logging.getLogger()
    root_logger.handlers = []

    if log_location is None:
        # basic config
        logging.basicConfig(
            stream=sys.stdout,
            format="%(asctime)s %(process)-05d/%(threadName)-10s [%(name)14s:%(lineno)3d] %(levelname)s: %(message)s")
    else:
        logging.basicConfig(
            filename=log_location,
            format="%(asctime)s %(process)-05d/%(threadName)-10s [%(name)14s:%(lineno)3d] %(levelname)s: %(message)s")

    all_loggers = logging.Logger.manager.loggerDict
    for logger in all_loggers.values():
        if isinstance(logger, logging.Logger):
            logger.setLevel(logging.WARNING)

    # configure individual loggers
    if config is not None:
        try:
            for name, value in config.items("Logging"):
                logging.getLogger(name).setLevel(logging._levelNames[value])
        except:
            log.exception("Reading logging config")


class Binding:
    '''
    This class represents a link between two paths to be synchronized. It
    encapsulates the source and destination directories, as well as the host
    name if the sync is remote.
    '''
    def __init__(self, source, dest, host=None):
        '''
        @param source - directory to be monitored, and to be replicated in
                        destination
        @param dest - destination directory which will receive all new changes
                      from source
        @param host - name of the host computer containing the destination
                      directory
        '''
        if None in (source, dest):
            raise ValueError("Source and destination can't be None")

        self._source = source
        self._dest = dest
        self._host = host

    @classmethod
    def make_local(cls, source, dest):
        '''
        Creates an instance of a local binding.

        @param source - directory to be monitored, and to be replicated in
                        destination
        @param dest - destination directory which will receive all new changes
                      from source
        '''
        return cls(source, dest)

    @classmethod
    def make_remote(cls, source, dest, host):
        '''
        Creates an instance of a remote binding.

        @param source - directory to be monitored, and to be replicated in
                        destination
        @param dest - destination directory which will receive all new changes
                      from source
        @param host - name of the host computer containing the destination
                      directory
        '''
        return cls(source, dest, host)

    @property
    def is_remote(self):
        '''
        Is this binding remote?
        '''
        return self._host is not None

    @property
    def source(self):
        '''
        Source directory.
        '''
        return self._source

    @property
    def dest_path(self):
        '''
        Path to the destination directory. If this is a remote binding the path
        will contain the hostname, otherwise it will be equivalent to
        destination directory
        '''
        if self.is_remote:
            dest_path = "{0}:{1}".format(self._host, self._dest)
            return dest_path
        else:
            return self._dest

    def __repr__(self):
        '''
        String representation of this binding.
        '''
        return "{0} -> {1}".format(self.source, self.dest_path)


class FileExcluder:
    '''
    This class reads the exclude file and collects all file extensions that
    should be ignored by the syncer. Execute an instance of this class to check
    if file passes this filter.
    '''
    def __init__(self, config_path):
        self._file_exts = self._read_config(config_path)

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

    def is_excluded(self, file_path):
        '''
        An alias for calling this instance directly.
        '''
        return self.__call__(file_path)

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


class EventSink:
    '''
    This class is responsible for aggregating multiple file changes, and
    issuing rsync calls at a set interval. If the number of files to sync
    exceeds the threshold rsync will automatically be issued.
    '''
    SYNC_THRESHOLD = 1000
    def __init__(self, auto_sync, file_excluder, loop=None):
        '''
        Args:
            auto_sync - AutoSync instance
            file_excluder - filter of files to be excluded from synching
            loop - asyncio loop
        '''
        self._auto_sync = auto_sync
        self._file_excluder = file_excluder
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._events = {}

    def sink(self, binding, event):
        '''
        This method accepts file events associated with the provided binding
        instance.

        @param binding - an instance of binding describing what folders are
                         being synchronized
        @param event - a pyinotify event indicating file read, write, or
                       creation etc.
        '''
        log.debug("File event: {0}".format(event.pathname))
        filename = os.path.basename(event.pathname)
        if self._file_excluder.is_excluded(filename):
            return

        binding_events = self._events.setdefault(binding, set())
        binding_events.add(event.pathname + '\n')

        # In case number of files that changed has exceeded the threshold
        # schedule a sync of this sync binding
        if len(binding_events) >= EventSink.SYNC_THRESHOLD:
            popped_events = self.pop_events(binding)
            asyncio.async(self._auto_sync._synchronize(binding, popped_events))
        else:
            self._auto_sync.schedule_sync()

    @property
    def all_events(self):
        '''
        All events that have been sinked to this sinker in the form of a
        dictionary.
        '''
        return self._events

    def pop_events(self, binding):
        '''
        Pop all events associated with this binding.

        @param binding - sync binding to pop events for
        '''
        return self._events.pop(binding, None)


class AutoSync:
    '''
    Class responsible for monitoring directories, and issuing synchronization
    calls.
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
        excluder = FileExcluder(AutoSync.CONFIG_PATH)
        self._event_sink = EventSink(self, excluder, loop)
        self._sync_handle = None
        self._bindings = {}

    def schedule_sync(self, delay=1.0):
        '''
        Schedules a call to do_sync to be run in delay seconds. If one is
        already scheduled it is a no-op.

        @param delay - how long to wait before syncing
        '''
        if self._sync_handle is None:
            self._sync_handle = self._loop.call_later(delay, self._do_sync)

    def add_sync_binding(self, sync_from, sync_to, host, events):
        '''
        Adds a monitor to watch sync_from. Destination gets updated when sync_from
        changes. sync_from and sync_to must be directories.

        @param sync_from - sync_from directory
        @param sync_to - destination directory
        @param host - server on which sync_to is located
        @param events - set of events which trigger this syncher
        '''
        def make_canonical(path):
            if path[-1] != '/':
                path += '/'
            return path

        if not os.path.isdir(sync_from):
            raise ValueError("Sync from parameter must be a directory.")

        # apply canonical form of directory path
        sync_from = make_canonical(sync_from)

        if host is not None:
            binding = Binding.make_remote(sync_from, sync_to, host)
        else:
            binding = Binding.make_local(sync_from, sync_to)

        log.debug("Adding sync binding: {0}".format(binding))

        partial_sink = functools.partial(self._event_sink.sink, binding)
        self._bindings[binding] = self._wm.add_watch(sync_from,
                                                     events,
                                                     proc_fun=partial_sink,
                                                     rec=True,
                                                     auto_add=True)
        log.debug("Current bindings: %s", self._bindings)

    def _do_sync(self):
        '''
        This method invokes rsync for all bindings in event sink.
        It automatically reschedules itself to be run every second. It should
        not be called directly.
        '''
        log.debug("Performing full synchronization...")
        for binding in self._bindings.keys():
            file_events = self._event_sink.pop_events(binding)
            if file_events is not None:
                asyncio.async(self._synchronize(binding, file_events))

        self._sync_handle = None

    @asyncio.coroutine
    def _synchronize(self, binding, changed_files):
        '''
        This method invokes rsync and synchronizes the bound directories.

        @param event - write, read, create etc event.
        '''
        create = asyncio.create_subprocess_exec(RSYNC,
                                                '--links',
                                                '-av',
                                                '--files-from=-',
                                                binding.source,
                                                binding.dest_path,
                                                stdin=asyncio.subprocess.PIPE,
                                                stdout=asyncio.subprocess.PIPE)
        proc = yield from create

        def sanitize_path(root, sub_path):
            '''
            Removes the root directory from sub_path, as in:
                root: /tmp/source
                sub_path: /tmp/source/dir/file
                result: dir/file
            Applies utf-8 encoding to the resultant string.
            '''
            clean_path = sub_path.split(root)[1]
            return clean_path.encode()

        encoded_files = (sanitize_path(binding.source, v) for v in changed_files)
        proc.stdin.writelines(encoded_files)
        yield from proc.stdin.drain()
        proc.stdin.write_eof()

        data = yield from proc.stdout.readline()
        line = data.decode('ascii').rstrip()

        yield from proc.wait()


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
                log.debug("Got new path message: {0}".format(msg_dict))
                sync_from = msg_dict['sync_from']
                sync_to = msg_dict['sync_to']
                events = msg_dict['events']
                remote_host = msg_dict.get('remote_host', None)

                self._auto_sync.add_sync_binding(sync_from,
                                                 sync_to,
                                                 remote_host,
                                                 events)

            response = {"success" : True, "error" : None}
            self._sync_rep.send(pickle.dumps(response))

        except Exception as e:
            log.error(e)
            error_msg = {"success" : False, "error" : e}
            self._sync_rep.send(pickle.dumps(error_msg))


def terminate(*args):
    log.warning("Eeeeh, supposedly terminating.")

def reload_program_config(*args):
    log.warn("Reloading the config")


def main():
    loop = asyncio.get_event_loop()
    auto_sync_controller = AutoSyncController(loop)

    set_up_logging()#log_location="/tmp/sync.log")
    log.setLevel(logging.DEBUG)
    try:
        log.info("Awaiting commands.")
        loop.run_forever()
    except (SystemExit, KeyboardInterrupt):
        log.info("Exiting...")


if __name__ == "__main__":
    try:
#        context = daemon.DaemonContext(
#            working_directory='/var/lib/autosync',
#            umask=0o002,
#            pidfile=lockfile.FileLock('/var/run/autosync.pid')
#        )
#        context.signal_map = {
#            signal.SIGTERM: loop.stop,
#            signal.SIGHUP: 'terminate',
#            signal.SIGUSR1: reload_program_config
#        }
#        context.gid = grp.getgrnam('mail').gr_gid
#        with context:
#            logging.basicConfig(filename="/tmp/sync.log",
#                                format='%(asctime)s %(message)s',
#                                level=logging.INFO)

#            log.info("Mercy shmercy!")
            main()
    except Exception as e:
        log.exception("Couldn't start: {0}".format(e))
        print (e)

