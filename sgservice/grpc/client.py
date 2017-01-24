import grpc
from api.volumes import VolumeClient


class ControlClient(object):
    def __init__(self, host, port):
        channel = grpc.insecure_channel(
            '%(host)s:%(port)s' % {'host': host,
                                   'port': port})
        self.volumes = VolumeClient(channel=channel)
