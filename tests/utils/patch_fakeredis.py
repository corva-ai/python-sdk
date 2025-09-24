from fakeredis._commands import command
from fakeredis._fakesocket import FakeSocket
from fakeredis._server import FakeServer


@command(name="info", fixed=(), repeat=(bytes,), flags=[])
def info(self, *sections):
    info_data = {"redis_version": "7.4.0"}
    lines = [f"{k}:{v}" for k, v in info_data.items()]
    return "\r\n".join(lines) + "\r\n"

FakeSocket.info = info  # type: ignore
FakeServer._socket_cls = FakeSocket  # type: ignore
