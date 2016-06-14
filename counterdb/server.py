"""
File: server.py
A basic tcp server built using asyncio. Generic enough to be used by both the
node system, and the public facing api.
"""


import asyncio
import socket
import msgpack
import logging
from aiohttp import web
import json


logger = logging.getLogger('counterdb.server')


class HTTPServer(object):
    def __init__(self, loop, host, port, counters):
        self.loop = loop
        self.port = port
        self.host = host
        self.counters = counters
        self.app = web.Application()
        self.app.router.add_route('GET', '/', self.handle_slash)
        self.app.router.add_route('GET', '/{key}', self.handle_get)
        self.app.router.add_route('POST', '/{key}', self.handle_post)
        self.srv = None

    async def handle_get(self, request):
        key = request.match_info.get('key', None)
        if key is not None:
            key = key[:15]
            count = self.counters.get(key)
            return web.Response(body=json.dumps({key: count}).encode('utf-8'))

    async def handle_slash(self, request):
        counters = self.counters.get_all()
        return web.Response(body=json.dumps(counters).encode('utf-8'))

    async def handle_post(self, request):
        key = request.match_info.get('key', None)
        if key is not None:
            key = key[:15]
            self.counters.increment(key)
            return web.Response(body=json.dumps({'status': 'yolo'}).encode('utf-8'))
        else:
            counters = self.counters.get_all()
            return web.HTTPBadRequest()

    async def serve(self):
        srv = await self.loop.create_server(
            self.app.make_handler(), self.host, self.port)
        return srv


class TCPServer(object):
    def __init__(self, loop, addr, port, callback):
        self.loop = loop
        self.addr = addr
        self.port = port
        self.callback = callback
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._shutdown = False

    def shutdown(self):
        self.shutdown = True

    async def _handler(self, client):
        """
        Handler callback passed into TCPServer. This deals with decoding
        things from msgpack and passing those into the user defined callback.
        """
        with client:
            unpacker = msgpack.Unpacker(encoding='utf-8')
            while True:
                data = await self.loop.sock_recv(client, 512)
                if data is None or data == b'':
                    break
                unpacker.feed(data)
                for msg in unpacker:
                    # Actually do the thing
                    self.loop.create_task(self.callback(self.loop, msg))

    async def serve(self):
        self.sock.bind((self.addr, self.port))
        self.sock.listen(32)
        self.sock.setblocking(False)
        with self.sock:
            while True:
                client, addr = await self.loop.sock_accept(self.sock)
                self.loop.create_task(self._handler(client))
                if self._shutdown:
                    break
