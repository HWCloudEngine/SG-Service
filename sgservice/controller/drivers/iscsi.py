# Copyright 2012, Red Hat, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from oslo_config import cfg
from oslo_log import log as logging
from oslo_serialization import jsonutils

from sgservice.common import constants
from sgservice.controller.sgdriver import SGDriver
from sgservice import exception
from sgservice.i18n import _, _LE
from sgservice.objects import fields

from sg_control.backup_ctrl import BackupCtrl
from sg_control.control_api import common_pb2
from sg_control.control_api import backup_pb2
from sg_control.control_api import replicate_pb2
from sg_control.control_api import snapshot_pb2
from sg_control.control_api import volume_pb2
from sg_control.replicate_ctrl import ReplicateCtrl
from sg_control.snap_ctrl import SnapCtrl
from sg_control.volume_ctrl import VolumeCtrl

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

VOLUME_STATUS_MAPPING = {
    volume_pb2.VOL_AVAILABLE: fields.VolumeStatus.ENABLED,
    volume_pb2.VOL_UNKNOWN: fields.VolumeStatus.ERROR,
    volume_pb2.VOL_ENABLING: fields.VolumeStatus.ENABLING,
    volume_pb2.VOL_DELETED: fields.VolumeStatus.DELETED
}

BACKUP_STATUS_MAPPING = {
    backup_pb2.BACKUP_CREATING: fields.BackupStatus.CREATING,
    backup_pb2.BACKUP_AVAILABLE: fields.BackupStatus.AVAILABLE,
    backup_pb2.BACKUP_DELETING: fields.BackupStatus.DELETING,
    backup_pb2.BACKUP_DELETED: fields.BackupStatus.DELETED,
    backup_pb2.BACKUP_RESTORING: fields.BackupStatus.RESTORING
}

SNAPSHOT_STATUS_MAPPING = {
    snapshot_pb2.SNAP_CREATED: fields.SnapshotStatus.AVAILABLE,
    snapshot_pb2.SNAP_CREATING: fields.SnapshotStatus.CREATING,
    snapshot_pb2.SNAP_DELETING: fields.SnapshotStatus.DELETING,
    snapshot_pb2.SNAP_INVALID: fields.SnapshotStatus.ERROR,
    snapshot_pb2.SNAP_ROLLBACKING: fields.SnapshotStatus.ROLLING_BACK,
    snapshot_pb2.SNAP_ROLLBACKED: fields.SnapshotStatus.AVAILABLE,
    snapshot_pb2.SNAP_DELETED: fields.SnapshotStatus.DELETED
}

REPLICATE_STATUS_MAPPING = {
    replicate_pb2.REP_UNKNOW: None,
    replicate_pb2.REP_CREATING: fields.ReplicateStatus.CREATING,
    replicate_pb2.REP_ENABLING: fields.ReplicateStatus.ENABLING,
    replicate_pb2.REP_ENABLED: fields.ReplicateStatus.ENABLED,
    replicate_pb2.REP_DISABLING: fields.ReplicateStatus.DISABLING,
    replicate_pb2.REP_DISABLED: fields.ReplicateStatus.DISABLED,
    replicate_pb2.REP_FAILING_OVER: fields.ReplicateStatus.FAILING_OVER,
    replicate_pb2.REP_FAILED_OVER: fields.ReplicateStatus.FAILED_OVER,
    replicate_pb2.REP_REVERSING: fields.ReplicateStatus.REVERSING,
    replicate_pb2.REP_DELETING: fields.ReplicateStatus.DELETING,
    replicate_pb2.REP_DELETED: fields.ReplicateStatus.DELETED,
    replicate_pb2.REP_ERROR: fields.ReplicateStatus.ERROR
}

REPLICATE_ROLE_MAPPING = {
    constants.REP_MASTER: replicate_pb2.REP_PRIMARY,
    constants.REP_SLAVE: replicate_pb2.REP_SECONDARY
}

ROLE_REPLICATE_MAPPING = {
    replicate_pb2.REP_PRIMARY: constants.REP_MASTER,
    replicate_pb2.REP_SECONDARY: constants.REP_SLAVE
}

CLIENT_MODE_MAPPING = {
    constants.AGENT_MODE: common_pb2.AGENT_MODE,
    constants.ISCSI_MODE: common_pb2.ISCSI_MODE
}

GB_SIZE = 1024 * 1024 * 1024


class ISCSIDriver(SGDriver):
    def __init__(self):
        super(ISCSIDriver, self).__init__()

    def volume_ctrl(self, sg_client):
        return VolumeCtrl(sg_client.host, sg_client.port)

    def backup_ctrl(self, sg_client):
        return BackupCtrl(sg_client.host, sg_client.port)

    def snap_ctrl(self, sg_client):
        return SnapCtrl(sg_client.host, sg_client.port)

    def replicate_ctrl(self, sg_client):
        return ReplicateCtrl(sg_client.host, sg_client.port)

    def list_devices(self, sg_client):
        try:
            res = self.volume_ctrl(sg_client).ListDevices()
        except Exception as exc:
            msg = _LE('list devices failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('list devices failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)
        return res.devices

    def enable_sg(self, sg_client, volume, device):
        vol_size = volume.size * GB_SIZE
        try:
            res = self.volume_ctrl(sg_client).EnableSG(
                volume.id, vol_size, device)
        except Exception as exc:
            msg = _LE('enable sg failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('enable sg failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def disable_sg(self, sg_client, volume):
        try:
            res = self.volume_ctrl(sg_client).DisableSG(volume.id)
        except Exception as exc:
            msg = _LE('disable sg failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('disable sg failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def get_volume(self, sg_client, volume):
        try:
            res = self.volume_ctrl(sg_client).GetVolume(volume.id)
        except Exception as exc:
            msg = _LE('get volume  failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            volume = {'id': volume.id,
                      'status': fields.VolumeStatus.DELETED,
                      'replicate_status': fields.ReplicateStatus.DELETED}
            return volume

        status = VOLUME_STATUS_MAPPING[res.volume.vol_status]
        replicate_status = REPLICATE_STATUS_MAPPING[res.volume.rep_status]

        volume = {
            'id': volume.id,
            'status': status,
            'replicate_status': replicate_status,
            'replicate_mode': ROLE_REPLICATE_MAPPING.get(res.volume.role,
                                                         None)}
        return volume

    def list_volumes(self):
        pass

    def initialize_connection(self, sg_client, volume, mode):
        mode = CLIENT_MODE_MAPPING.get(mode, common_pb2.ISCSI_MODE)
        try:
            res = self.volume_ctrl(sg_client).InitializeConnection(
                volume.id, mode)
        except Exception as exc:
            msg = _LE('initialize connection failed, err:%s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('initialize connection failed, err_no:%s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)
        target_portal = "%s:3260" % sg_client.host
        target_iqn = res.connection_info['target_iqn']
        target_lun = int(res.connection_info['target_lun'])
        connection_info = {
            'target_portal': target_portal,
            'target_iqn': target_iqn,
            'target_lun': target_lun,
        }
        return connection_info

    def terminate_connection(self, sg_client, volume, mode, device=None):
        mode = CLIENT_MODE_MAPPING.get(mode, common_pb2.ISCSI_MODE)
        try:
            res = self.volume_ctrl(sg_client).TerminateConnection(
                volume.id, mode, device)
        except Exception as exc:
            msg = _LE('terminate connection failed, err:%s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('terminate connection failed, err_no:%s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def attach_volume(self, sg_client, volume, device):
        try:
            res = self.volume_ctrl(sg_client).AttachVolume(volume.id, device)
        except Exception as exc:
            msg = _LE('attach volume failed, err:%s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('attach volume failed, err_no:%s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def detach_volume(self, sg_client, volume):
        try:
            res = self.volume_ctrl(sg_client).DetachVolume(volume.id)
        except Exception as exc:
            msg = _LE('detach volume failed, err:%s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('detach volume failed, err_no:%s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def create_backup(self, sg_client, backup):
        # backup_mode: full, incremental
        backup_mode = backup.type
        # backup_type: local, remote
        backup_type = backup.destination
        vol_id = backup.volume_id
        vol_size = backup.size * GB_SIZE
        backup_id = backup.id

        try:
            res = self.backup_ctrl(sg_client).CreateBackup(
                backup_mode, backup_type, vol_id, vol_size, backup_id)
        except Exception as exc:
            msg = _LE('create backup failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('create backup failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def delete_backup(self, sg_client, backup):
        vol_id = backup.volume_id
        backup_id = backup.id

        try:
            res = self.backup_ctrl(sg_client).DeleteBackup(backup_id, vol_id)
        except Exception as exc:
            msg = _LE('delete backup failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('delete backup failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def restore_backup(self, sg_client, backup, restore_volume, device):
        driver_data = jsonutils.loads(backup.driver_data)
        vol_id = driver_data['volume_id']
        vol_size = backup['size']
        backup_id = driver_data['backup_id']
        # backup_type: local, remote
        backup_type = backup['destination']
        # new volume id
        new_vol_id = restore_volume.id
        new_vol_size = restore_volume.size * GB_SIZE
        new_device = device

        try:
            res = self.backup_ctrl(sg_client).RestoreBackup(
                backup_id, backup_type, vol_id, vol_size, new_vol_id,
                new_vol_size, new_device)
        except Exception as exc:
            msg = _LE('restore backup failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('restore backup failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def get_backup(self, sg_client, backup):
        vol_id = backup.volume_id
        backup_id = backup.id

        try:
            res = self.backup_ctrl(sg_client).GetBackup(backup_id, vol_id)
        except Exception as exc:
            msg = _LE('get backup failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            if res.status == common_pb2.sBackupNotExist:
                backup = {'id': backup.id,
                          'status': fields.BackupStatus.DELETED}
            else:
                msg = _LE('get backup failed, err_no: %s' % res.status)
                LOG.error(msg)
                raise exception.SGDriverError(reason=msg)
        else:
            backup = {'id': backup.id,
                      'status': BACKUP_STATUS_MAPPING[res.backup_status]}
        return backup

    def list_backups(self, volume):
        pass

    def create_snapshot(self, sg_client, snapshot, volume):
        snap_type = snapshot.destination
        if snapshot.checkpoint_id:
            checkpoint_uuid = snapshot.checkpoint_id
        else:
            checkpoint_uuid = None
        vol_id = snapshot.volume_id
        snap_id = snapshot.id

        try:
            res = self.snap_ctrl(sg_client).CreateSnapshot(
                snap_type, vol_id, snap_id, checkpoint_uuid)
        except Exception as exc:
            msg = _LE('create snapshot failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.header.status != 0:
            msg = _LE('create snapshot failed, err_no: %s' % res.header.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def delete_snapshot(self, sg_client, snapshot):
        vol_id = snapshot.volume_id
        snap_id = snapshot.id
        snap_type = snapshot.destination
        if snapshot.checkpoint_id:
            checkpoint_uuid = snapshot.checkpoint_id
        else:
            checkpoint_uuid = None

        try:
            res = self.snap_ctrl(sg_client).DeleteSnapshot(
                snap_type, vol_id, snap_id, checkpoint_uuid)
        except Exception as exc:
            msg = _LE('delete snapshot failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.header.status != 0:
            msg = _LE('delete snapshot failed, err_no: %s' % res.header.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def rollback_snapshot(self, sg_client, snapshot):
        vol_id = snapshot.volume_id
        snap_id = snapshot.id
        snap_type = snapshot.destination
        if snapshot.checkpoint_id:
            checkpoint_uuid = snapshot.checkpoint_id
        else:
            checkpoint_uuid = None

        try:
            res = self.snap_ctrl(sg_client).RollbackSnapshot(
                snap_type, vol_id, snap_id, checkpoint_uuid)
        except Exception as exc:
            msg = _LE('rollback snapshot failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.header.status != 0:
            msg = _LE('rollback snapshot failed, err_no: %s' %
                      res.header.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def get_snapshot(self, sg_client, snapshot):
        vol_id = snapshot.volume_id
        snap_id = snapshot.id

        try:
            res = self.snap_ctrl(sg_client).GetSnapshot(vol_id, snap_id)
        except Exception as exc:
            msg = _LE('get snapshot failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.header.status != 0:
            if res.header.status == common_pb2.sSnapNotExist:
                snapshot = {'id': snapshot.id,
                            'status': fields.SnapshotStatus.DELETED}
            else:
                msg = _LE('get snapshot failed, err_no: %s' %
                          res.header.status)
                LOG.error(msg)
                raise exception.SGDriverError(reason=msg)
        else:
            snapshot = {'id': snapshot.id,
                        'status': SNAPSHOT_STATUS_MAPPING[res.snap_status]}
        return snapshot

    def list_snapshots(self, volume):
        pass

    def create_volume_from_snapshot(self, sg_client, snapshot, new_volume_id,
                                    device):
        try:
            res = self.snap_ctrl(sg_client).CreateVolumeFromSnap(
                snapshot.volume_id, snapshot.id, new_volume_id, device)
        except Exception as exc:
            msg = _LE('create volume from snapshot failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.header.status != 0:
            msg = _LE('create volume from snapshot failed, '
                      'err_noe: %s' % res.header.status)
            raise exception.SGDriverError(reason=msg)

    def query_volume_from_snapshot(self, sg_client, new_volume_id):
        try:
            res = self.snap_ctrl(sg_client).QueryVolumeFromSnap(new_volume_id)
        except Exception as exc:
            msg = _LE('query volume from snapshot failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.header.status != 0:
            msg = _LE('query volume from snapshot failed, '
                      'err_noe: %s' % res.header.status)
            raise exception.SGDriverError(reason=msg)
        return {'id': new_volume_id,
                'status': VOLUME_STATUS_MAPPING[res.vol_status]}

    def create_replicate(self, sg_client, volume):
        rep_uuid = volume.replication_id
        role = REPLICATE_ROLE_MAPPING[volume.replicate_mode]
        local_volume = volume.id
        peer_volumes = [volume.peer_volume]
        try:
            res = self.replicate_ctrl(sg_client).CreateReplication(
                rep_uuid, local_volume, role, peer_volumes)
        except Exception as exc:
            msg = _LE('create replicate failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('create replicate failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def enable_replicate(self, sg_client, volume):
        vol_id = volume.id
        role = REPLICATE_ROLE_MAPPING[volume.replicate_mode]
        try:
            res = self.replicate_ctrl(sg_client).EnableReplication(vol_id,
                                                                   role)
        except Exception as exc:
            msg = _LE('enable replicate failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('enable replicate failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def disable_replicate(self, sg_client, volume):
        vol_id = volume.id
        role = REPLICATE_ROLE_MAPPING[volume.replicate_mode]
        try:
            res = self.replicate_ctrl(sg_client).DisableReplication(
                vol_id, role)
        except Exception as exc:
            msg = _LE('disable replicate failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('disable replicate failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def failover_replicate(self, sg_client, volume, checkpoint_id,
                           snapshot_id):
        vol_id = volume.id
        role = REPLICATE_ROLE_MAPPING[volume.replicate_mode]
        try:
            res = self.replicate_ctrl(sg_client).FailoverReplication(
                vol_id, role, checkpoint_id=checkpoint_id,
                snapshot_id=snapshot_id)
        except Exception as exc:
            msg = _LE('failover replicate failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('failover replicate failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def delete_replicate(self, sg_client, volume):
        vol_id = volume.id
        role = REPLICATE_ROLE_MAPPING[volume.replicate_mode]
        try:
            res = self.replicate_ctrl(sg_client).DeleteReplication(vol_id,
                                                                   role)
        except Exception as exc:
            msg = _LE('delete replicate failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('delete replicate failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def reverse_replicate(self, sg_client, volume):
        vol_id = volume.id
        role = REPLICATE_ROLE_MAPPING[volume.replicate_mode]
        try:
            res = self.replicate_ctrl(sg_client).ReverseReplication(
                vol_id, role)
        except Exception as exc:
            msg = _LE('reverse replicate failed, err: %s' % exc)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        if res.status != 0:
            msg = _LE('reverse replicate failed, err_no: %s' % res.status)
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)
