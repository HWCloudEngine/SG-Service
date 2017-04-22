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

from sgservice.controller.sgdriver import SGDriver
from sgservice import exception
from sgservice.i18n import _
from sgservice.objects import fields

sg_client_opts = [
    cfg.StrOpt('sg_client_host',
               help='The host of sg.'),
    cfg.StrOpt('sg_client_port',
               help='The gprc port of sg'),
]

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
CONF.register_opts(sg_client_opts, group='sg_client')

REPLICATE_STATUS_MAPPING = {
    fields.ReplicateStatus.ENABLING: fields.ReplicateStatus.ENABLED,
    fields.ReplicateStatus.DISABLING: fields.ReplicateStatus.DISABLED,
    fields.ReplicateStatus.DELETING: fields.ReplicateStatus.DELETED,
    fields.ReplicateStatus.FAILING_OVER: fields.ReplicateStatus.FAILED_OVER,
    fields.ReplicateStatus.REVERSING: fields.ReplicateStatus.DISABLED
}

BACKUP_STATUS_MAPPING = {
    fields.BackupStatus.CREATING: fields.BackupStatus.AVAILABLE,
    fields.BackupStatus.DELETING: fields.BackupStatus.DELETED,
    fields.BackupStatus.RESTORING: fields.BackupStatus.AVAILABLE
}

SNAPSHOT_STATUS_MAPPING = {
    fields.SnapshotStatus.CREATING: fields.SnapshotStatus.AVAILABLE,
    fields.SnapshotStatus.DELETING: fields.SnapshotStatus.DELETED,
    fields.SnapshotStatus.ROLLING_BACK: fields.SnapshotStatus.AVAILABLE
}

VOLUME_STATUS_MAPPING = {
    fields.VolumeStatus.ENABLING: fields.VolumeStatus.ENABLED,
    fields.VolumeStatus.DISABLING: fields.VolumeStatus.DISABLED
}


class FakeDriver(SGDriver):
    def __init__(self):
        super(FakeDriver, self).__init__()

    def list_devices(self):
        return ['/dev/sda', '/dev/sdb', '/dev/sdc', '/dev/sdd', '/dev/sde',
                '/dev/sdf', '/dev/sdg', '/dev/sdh', '/dev/sdi', '/dev/sdj',
                '/dev/sdk', '/dev/sdl', '/dev/sdm', '/dev/sdn', '/dev/sdo',
                '/dev/sdp', '/dev/sdq', '/dev/sdr', '/dev/sds', '/dev/sdt',
                '/dev/sdu', '/dev/sdv', '/dev/sdw', '/dev/sdx', '/dev/sdy',
                '/dev/sdz']

    def enable_sg(self, volume, device):
        driver_data = {
            'driver_type': 'iscsi',
            'target_iqn': 'iqn.2017-01.huawei.sgs.%s' % volume['id']
        }
        return driver_data

    def disable_sg(self, volume):
        pass

    def get_volume(self, volume):
        status = volume['status']
        if status is not None and status in VOLUME_STATUS_MAPPING.keys():
            status = VOLUME_STATUS_MAPPING[status]

        replicate_status = volume['replicate_status']
        if (replicate_status is not None and
                replicate_status in REPLICATE_STATUS_MAPPING.keys()):
            replicate_status = REPLICATE_STATUS_MAPPING[replicate_status]
        volume = {
            'id': volume['id'],
            'status': status,
            'replicate_status': replicate_status
        }
        return volume

    def list_volumes(self):
        pass

    def attach_volume(self, context, volume, instance_uuid,
                      host_name_sanitized, mountpoint, mode):
        pass

    def detach_volume(self, context, volume, attachment):
        pass

    def initialize_connection(self, context, volume, connector=None):
        driver_data = jsonutils.loads(volume['driver_data'])
        driver_type = driver_data['driver_type']
        connection_info = {}
        if driver_type == 'iscsi':
            # iscsi mode
            target_portal = "%s:3260" % CONF.sg_client.sg_client_host
            target_iqn = driver_data['target_iqn']
            target_lun = 1
            data = {
                "target_discovered": False,
                "target_portal": target_portal,
                "target_iqn": target_iqn,
                "target_lun": target_lun,
                "volume_id": volume.id,
                "display_name": volume.display_name,
            }
            connection_info = {'driver_volume_type': driver_type,
                               'data': data}
        return connection_info

    def create_backup(self, backup):
        pass

    def delete_backup(self, backup):
        pass

    def restore_backup(self, backup, restore_volume, device):
        pass

    def get_backup(self, backup):
        status = backup['status']
        if status is not None and status in BACKUP_STATUS_MAPPING.keys():
            status = BACKUP_STATUS_MAPPING[status]
        backup = {
            'id': backup['id'],
            'status': status
        }
        return backup

    def list_backups(self, volume):
        pass

    def create_snapshot(self, snapshot, volume):
        pass

    def delete_snapshot(self, snapshot):
        pass

    def rollback_snapshot(self, snapshot):
        pass

    def get_snapshot(self, snapshot):
        status = snapshot['status']
        if status is not None and status in SNAPSHOT_STATUS_MAPPING.keys():
            status = SNAPSHOT_STATUS_MAPPING[status]
        snapshot = {
            'id': snapshot['id'],
            'status': status
        }
        return snapshot

    def list_snapshots(self, volume):
        pass

    def create_volume_from_snapshot(self, snapshot, new_volume_id, device):
        pass

    def query_volume_from_snapshot(self, new_volume_id):
        return {
            'id': new_volume_id,
            'status': fields.VolumeStatus.AVAILABLE
        }

    def create_replicate(self, volume):
        pass

    def enable_replicate(self, volume):
        pass

    def disable_replicate(self, volume):
        pass

    def failover_replicate(self, volume, checkpoint_id, snapshot_id):
        pass

    def delete_replicate(self, volume):
        pass

    def reverse_replicate(self, volume):
        pass
