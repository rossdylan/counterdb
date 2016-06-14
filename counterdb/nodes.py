import etcd
import asyncio
import functools
import logging
import msgpack
import socket
import counterdb.server as server
from collections import defaultdict


logger = logging.getLogger('counterdb.nodes')


class NodeNameNotUnique(Exception):
    pass


"""
These _etcd methods are used to wrap the etcd client operations we use inside
of futures so they can be used safely with asyncio
"""
def _etcd_read(loop, client, key, **kwargs):
    def thingy():
        try:
            return client.read(key, **kwargs)
        except Exception as e:
            return None
    # return loop.run_in_executor(None, functools.partial(client.read, key, **kwargs))
    return loop.run_in_executor(None, thingy)


def _etcd_write(loop, client, key, val, **kwargs):
    return loop.run_in_executor(None, functools.partial(client.write, key, val, **kwargs))

def _etcd_delete(loop, client, key, **kwargs):
    return loop.run_in_executor(None, functools.partial(client.delete, key, **kwargs))


class NodeManager(object):
    """
    Mangement system for finding and talking to other counterdb nodes. Each
    node registers itself in etcd when it becomes active. Once active other
    nodes will begin pushing updates to this node. Other systems can subscribe
    to messages recieved from other nodes.
    """
    def __init__(self, name, shost, sport, ehost, eport, loop):
        self.loop = loop
        self.client = etcd.Client(host=ehost, port=eport)
        logger.debug('Connected to etcd on {0}:{1}'.format(ehost, eport))
        self.name = name
        self.shost = shost
        self.sport = sport
        self._server = None
        self.callbacks = defaultdict(list)

    def add_callback(self, msg_type, cb):
        """
        Add a callback for a specific message type
        """
        logger.debug('Adding callback "{0}" for type "{1}"'.format(cb, msg_type))
        self.callbacks[msg_type].append(cb)

    async def _handler(self, loop, msg):
        """
        Handler callback passed into TCPServer. It receives fully unpacked
        messages.
        """
        # Do The Thing.
        tasks = []
        print(msg)
        logger.debug('Handling message of type "{0}"'.format(msg['type']))
        if msg['type'] in self.callbacks:
            for cb in self.callbacks[msg['type']]:
                tasks.append(self.loop.create_task(cb(msg)))
        await asyncio.wait(tasks)

    async def set_active(self):
        """
        Set this node to active within etcd. Other nodes will attempt to
        replicate data to this node.
        """
        res = await _etcd_read(self.loop, self.client, '/nodes/{0}'.format(self.name))
        print(res)
        if res is not None:
            raise NodeNameNotUnique()

        self._server = server.TCPServer(self.loop, self.shost, self.sport, self._handler)
        server_task = self.loop.create_task(self._server.serve())
        logger.info("Starting node server on {0}:{1}".format(self.shost, self.sport))
        etcd_val = "{0}:{1}".format(self.shost, self.sport)
        await _etcd_write(self.loop, self.client, '/nodes/{0}'.format(self.name), etcd_val)
        logger.info('Setting self ({0}) active in etcd'.format(self.name))
        await server_task

    async def set_inactive(self):
        """
        Remove this node from etcd. Other nodes will no longer try and push
        updates to this node.
        """
        await _etcd_delete(self.loop, self.client, '/nodes/{0}'.format(self.name))
        self._server.shutdown()
        logger.info('Setting self ({0}) inactive in etcd'.format(self.name))

    async def get_nodes(self):
        """
        Grab the active nodes out of etcd and yield them.
        """
        results = await _etcd_read(self.loop, self.client, '/nodes', recursive=True)
        kvs = []
        for res in results.leaves:
            logger.debug('get_nodes -> {0}={1}'.format(res.key, res.value))
            kvs.append((res.key, res.value))
        return kvs

    async def _send_single(self, addr, msg):
        """
        Send a single message encoded via msgpack to the given addr
        """
        logger.debug('Sending "{0}" to "{1}"'.format(addr, msg))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(True)
        sock.connect(addr)
        with sock:
            await self.loop.sock_sendall(sock, msgpack.packb(msg))

    async def send(self, msg):
        """
        Sync the given message to all nodes
        """
        tasks = []
        nodes = await self.get_nodes()
        logger.debug('send() triggered to {0}'.format(nodes))
        for key, val in nodes:
            if key == '/nodes/{0}'.format(self.name):
                continue
            host, port = val.split(":")
            tasks.append(
                self.loop.create_task(
                    self._send_single((host, int(port)), msg)))
        await asyncio.wait(tasks)
