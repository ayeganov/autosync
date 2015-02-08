'''
This module provides functionality required to communicate with the AutoSync daemon.
'''
import argparse
import asyncio
import functools
import pickle
import sys

import pyinotify

import lib.async_zmq


EVENTS_MAP = {
    'c' : pyinotify.IN_CREATE,
    'm' : pyinotify.IN_MODIFY,
    'f' : pyinotify.IN_MOVED_FROM,
    't' : pyinotify.IN_MOVED_TO,
    'd' : pyinotify.IN_DELETE
}

EVENTS_DESC = """File events that would trigger the synchronization:
'c' - file created
'm' - file modified
'f' - file moved from a watched directory
't' - file moved to a watched directory
'd' - file deleted
"""


class SyncClient:
    '''
    Synchronization client for communicating with the auto sync daemon,
    creating, deleting, or modifying directory synchers.
    '''
    def __init__(self, loop):
        '''
        @param loop - asyncio loop
        '''
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._new_path_req = lib.async_zmq.SocketFactory.req_socket("/tmp/add_syncher",
                                                                    on_send=self._on_new_path_send,
                                                                    on_recv=self._on_new_path_recv,
                                                                    loop=self._loop)
        self._awaiting_ack = False
        self._last_msgs = None

    def create_new_path(self, sync_from, sync_to, events, server=None):
        '''
        Sends a message to auto sync daemon to watch a new path.

        @param sync_from - sync_from directory
        @param sync_to - destination directory
        @param events - string represting events which will trigger the new
                        path to be updated
        @param server - server on which sync_to is located
        '''
        if self._awaiting_ack:
            print("Previous path has not been processed yet, can't set new one yet.")
            return

        if events.strip() == "":
            print("You must specify some events to trigger synchronization.")
            self._loop.stop()
            return

        event_vals = (EVENTS_MAP[e] for e in events)
        event_mask = functools.reduce(lambda mask, cur: mask | cur, event_vals)
        new_path_msg = {
            "sync_from" : sync_from,
            "sync_to" : sync_to,
            "events" : event_mask
        }

        if server is not None:
            new_path_msg['remote_host'] = server

        self._new_path_req.send(pickle.dumps(new_path_msg))

    def _on_new_path_send(self, msgs):
        '''
        Callback triggered when messages are sent on _new_path_req.

        @param msgs - list of sent messages
        '''
        self._last_msgs = msgs
        self._awaiting_ack = True


    def _on_new_path_recv(self, msgs):
        '''
        Callback triggered when messages are received on _new_path_req.

        @param msgs - list of received messages
        '''
        msg = pickle.loads(msgs[-1])
        if not msg['success']:
            print(msg['error'])

        self._last_msgs = None
        self._awaiting_ack = False
        self._loop.stop()


def timeout(loop):
    '''
    This operation gets invoked after a certain amount of time passes and
    AutoSync daemon still has not responded.
    '''
    print("Operation timed out. Can't communicate with the AutoSync daemon.")
    loop.stop()

def main():
    parser = argparse.ArgumentParser(description="Client program to communicate "
                                                 "with the AutoSync daemon.",
                                     formatter_class=argparse.RawTextHelpFormatter)
    new_path_group = parser.add_argument_group("New Path",
                                               description="A set of arguments "
                                               "to create a new sync path.")

    new_path_group.add_argument("-f",
                                "--from_dir",
                                help="Directory from which to synchronize files.")

    new_path_group.add_argument("-t",
                                "--to_dir",
                                help="Directory to be updated when from_dir is changed.")

    new_path_group.add_argument("-s",
                                "--server",
                                help="Server on which to_dir is located.")

    new_path_group.add_argument("-e",
                                "--events",
                                help=EVENTS_DESC,
                                default="cm")

    args = parser.parse_args()

    try:
        loop = asyncio.get_event_loop()
        sync_client = SyncClient(loop=loop)

        if args.from_dir and args.to_dir and args.events:
            print("Creating new sync path: %s => %s at %s" % (args.from_dir,
                                                              args.to_dir,
                                                              args.events))
            loop.call_soon(sync_client.create_new_path,
                           args.from_dir,
                           args.to_dir,
                           args.events,
                           args.server)

            loop.call_later(2, timeout, loop)
        else:
            print("You must supply new path arguments.")
            sys.exit(0)

        loop.run_forever()
        print("Here")
    except (KeyboardInterrupt, SystemExit):
        print("Exiting...")


if __name__ == "__main__":
    main()
