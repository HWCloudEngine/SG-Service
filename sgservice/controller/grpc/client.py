import grpc
from api.backups import BackupClient
from api.replicates import ReplicateClient
from api.snapshots import SnapshotClient
from api.volumes import VolumeClient


class ControlClient(object):
    def __init__(self, host, port):
        channel = grpc.insecure_channel(
            '%(host)s:%(port)s' % {'host': host,
                                   'port': port})
        self.volumes = VolumeClient(channel=channel)
        self.replicates = ReplicateClient(channel=channel)
        self.snapshots = SnapshotClient(channel=channel)
        self.backups = BackupClient(channel=channel)
