from sgservice.common import constants
from sgservice.controller.grpc.control import common_pb2
from sgservice.controller.grpc.control import snapshot_control_pb2
from sgservice.controller.grpc.control import snapshot_control_pb2_grpc
from sgservice.controller.grpc.control import snapshot_pb2
from sgservice.objects import fields

SNAPSHOT_TYPE_MAPPING = {
    constants.LOCAL_SNAPSHOT: snapshot_pb2.SNAP_LOCAL,
    constants.REMOTE_SNAPSHOT: snapshot_pb2.SNAP_REMOTE
}

SNAPSHOT_STATUS_MAPPING = {
    snapshot_pb2.SNAP_CREATING: fields.SnapshotStatus.CREATING,
    snapshot_pb2.SNAP_CREATED: fields.SnapshotStatus.AVAILABLE,
    snapshot_pb2.SNAP_DELETING: fields.SnapshotStatus.DELETING,
    snapshot_pb2.SNAP_DELETED: fields.SnapshotStatus.DELETED,
    snapshot_pb2.SNAP_ROLLBACKING: fields.SnapshotStatus.ROLLING_BACK,
    snapshot_pb2.SNAP_ROLLBACKED: fields.SnapshotStatus.AVAILABLE,
    snapshot_pb2.SNAP_INVALID: fields.SnapshotStatus.ERROR
}


class SnapshotClient(object):
    def __init__(self, channel):
        self.stub = snapshot_control_pb2_grpc.SnapshotControlStub(channel)

    def create_snapshot(self, snapshot, volume):
        snap_type = SNAPSHOT_STATUS_MAPPING[snapshot['destination']]
        checkpoint_id = snapshot['checkpoint_id']
        snap_name = snapshot['id']

        replication_id = volume['replication_id']
        vol_size = volume['size']
        vol_name = volume['id']

        header = snapshot_pb2.SnapReqHead(
            snap_type=snap_type,
            replication_uuid=replication_id,
            checkpoint_uuid=checkpoint_id)
        req = snapshot_control_pb2.CreateSnapshotReq(
            header=header,
            vol_name=vol_name,
            vol_size=vol_size,
            snap_name=snap_name)

        response = self.stub.CreateSnapshot(req)
        return {'status': response.header.status}

    def list_snapshots(self, volume):
        vol_name = volume['id']
        header = snapshot_pb2.SnapReqHead()
        req = snapshot_control_pb2.ListSnapshotReq(
            header=header,
            vol_name=vol_name)

        response = self.stub.ListSnapshot(req)
        if response.header.status == common_pb2.sOk:
            snapshots = []
            for item in response.snap_name:
                snapshots.append({'id': item})
            return {'status': 0, 'snapshots': snapshots}
        else:
            return {'status': response.header.status}

    def get_snapshot(self, snapshot):
        vol_name = snapshot['volume_id']
        snap_name = snapshot['id']
        header = snapshot_pb2.SnapReqHead()
        req = snapshot_control_pb2.QuerySnapshotReq(
            header=header,
            vol_name=vol_name,
            snap_name=snap_name)

        response = self.stub.QuerySnapshot(req)
        if response.header.status == common_pb2.sOk:
            snapshot = {
                'id': snapshot['id'],
                'status': response.snap_status
            }
            return {'status': 0, 'snapshot': snapshot}
        else:
            return {'status': response.header.status}

    def rollback_snapshot(self, snapshot):
        vol_name = snapshot['volume_id']
        snap_name = snapshot['id']
        header = snapshot_pb2.SnapReqHead()
        req = snapshot_control_pb2.RollbackSnapshotReq(
            header=header,
            vol_name=vol_name,
            snap_name=snap_name)

        response = self.stub.RollbackSnapshot(req)
        return {'status': response.header.status}

    def delete_snapshot(self, snapshot):
        vol_name = snapshot['volume_id']
        snap_name = snapshot['id']
        header = snapshot_pb2.SnapReqHead()
        req = snapshot_control_pb2.DeleteSnapshotReq(
            header=header,
            vol_name=vol_name,
            snap_name=snap_name)

        response = self.stub.DeleteSnapshot(req)
        return {'status': response.header.status}
