import asyncio
import uvloop
import logging
import argparse
import socket
from .counters import CounterManager
from .nodes import NodeManager
import random
import sys
import signal
from urllib.parse import urlparse


logger = logging.getLogger('counterdb')


class CounterDB(object):
    def __init__(self, loop, name, etcd_uri, dbfile, nhost, nport, whost, wport):
        self.loop = loop
        self.name = name
        self.etcd_uri = etcd_uri
        self.dbfile = dbfile
        self.nhost = nhost
        self.nport = nport
        self.whost = whost
        self.wport = wport
        self.counters = CounterManager(self.name)
        self.webserver = server.HTTPServer(self.loop, whost, wport, self.counters)
        tmp = urlparse(etcd_uri)
        sp = tmp.netloc.split(':')
        eport = 2379
        ehost = sp[0]
        if len(sp) == 2:
            eport = int(sp[1])
        self.nodes = NodeManager(self.name,
                                 self.nhost,
                                 self.nport,
                                 ehost,
                                 eport,
                                 loop)
        self.nodes.add_callback('sync', self.sync_callback)
        self.timer_offset = (random.random() * 10) + 5
        self.running = False


    async def serve(self):
        self.loop.add_signal_handler(signal.SIGINT, self.sigint_handler)
        self.running = True
        nserver = self.loop.create_task(self.nodes.set_active())
        sync = self.loop.create_task(self.send_sync())
        wserver = self.loop.create_task(self.webserver.serve())
        while self.running:
            await asyncio.wait([sync, nserver, wserver], timeout=10)
        await self.loop.create_task(self.nodes.set_inactive())
        wserver.cancel()
        sync.cancel()
        nserver.cancel()

    def shutdown(self):
        self.running = False

    async def send_sync(self):
        """
        Send our view of the counters to all nodes we know about,
        wait for them to accept the msg
        """
        logger.debug('sync timer set to {0}'.format(self.timer_offset))
        while self.running:
            await asyncio.sleep(self.timer_offset)
            msg = {
                'type': 'sync',
                'contents': self.counters.to_dict()
            }
            await self.nodes.send(msg)
            logger.debug('Sync completed')

    def sigint_handler(self):
        self.running = False

    async def sync_callback(self, msg):
        """
        Handle a message with another nodes view of the world which we merge
        into ours.
        """
        self.counters.merge(msg['contents'])

def main():
    parser = argparse.ArgumentParser(description="Distributed Counting DB.")
    parser.add_argument('--logfile',
                        dest='logfile',
                        type=str,
                        default='./counterdb.log',
                        help='File to output logs to')
    parser.add_argument('--dbfile',
                        dest='dbfile',
                        type=str,
                        default='./counterdb.lmdb')
    parser.add_argument('--name',
                        dest='name',
                        type=str,
                        default=socket.gethostname())
    parser.add_argument('--host',
                        dest='host',
                        type=str,
                        default='127.0.0.1')
    parser.add_argument('--port',
                        dest='port',
                        type=int,
                        default=4242)
    parser.add_argument('--whost',
                        dest='whost',
                        type=str,
                        default='127.0.0.1')
    parser.add_argument('--wport',
                        dest='wport',
                        type=int,
                        default=8080)
    parser.add_argument('--etcduri',
                        dest='etcduri',
                        type=str,
                        default='etcd://localhost:2379')
    parser.add_argument('--verbose', '-v', action='count', default=1)
    args = parser.parse_args(sys.argv[1:])

    if args.verbose == 0:
       level = logging.ERROR
    if args.verbose == 1:
        level = logging.WARN
    if args.verbose == 2:
        level = logging.INFO
    if args.verbose > 2:
        level = logging.DEBUG
    logger.setLevel(level)

    log_file = logging.FileHandler(args.logfile)
    log_file.setLevel(level)

    log_stdo = logging.StreamHandler()
    log_stdo.setLevel(level)

    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_file.setFormatter(log_format)
    log_stdo.setFormatter(log_format)
    logger.addHandler(log_file)
    logger.addHandler(log_stdo)

    logger.info("Setting log level to {0}".format(args.verbose))
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    cdb_server = CounterDB(loop,
                           args.name,
                           args.etcduri,
                           args.dbfile,
                           args.host,
                           args.port,
                           args.whost,
                           args.wport)
    loop.run_until_complete(cdb_server.serve())
