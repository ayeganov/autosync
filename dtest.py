import logging
import os
import time

import daemon
import lockfile

log = logging.getLogger(__name__)


def main():
    while True:
        log.info("Still here: {0}".format(os.getpid()))
        time.sleep(1)


if __name__ == "__main__":
    try:
        log.info("starting")
        context = daemon.DaemonContext(
#            working_directory='/var/lib/autosync',
#            umask=0x2,
#            pidfile=lockfile.FileLock('/var/run/test.pid')
        )
        with context:
            logging.basicConfig(filename='/tmp/test.log',
                            format='%(asctime)s %(message)s',
                            level=logging.INFO)
            print("shit")
            log.info("Started")
            main()
    except Exception:
        log.exception("Couldn't start")
