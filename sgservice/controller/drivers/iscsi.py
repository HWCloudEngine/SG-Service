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

from sgservice.controller.grpc.client import ControlClient
from sgservice.controller.sgdriver import SGDriver
from sgservice import exception
from sgservice.i18n import _

sg_client_opts = [
    cfg.StrOpt('sg_client_host',
               help='The host of sg.'),
    cfg.StrOpt('sg_client_port',
               help='The gprc port of sg'),
]

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
CONF.register_opts(sg_client_opts, group='sg_client')


class ISCSISGDriver(SGDriver):
    def __init__(self):
        super(ISCSISGDriver, self).__init__()
        self.sg_ctrl = ControlClient(CONF.sg_client.sg_client_host,
                                     CONF.sg_client.sg_client_port)

    def list_devices(self):
        response = self.sg_ctrl.volumes.list_devices()
        if response['status'] != 0:
            msg = _("Call list_devices to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        return response['devices']

    def enable_sg(self, volume, device):
        response = self.sg_ctrl.volumes.enable_sg(volume, device)
        if response['status'] != 0:
            msg = _("Call enable_sg to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        driver_data = {}
        for key, value in response['driver_data']:
            driver_data[key] = value
        return driver_data

    def disable_sg(self, volume):
        response = self.sg_ctrl.volumes.disable_sg(volume)
        if response['status'] != 0:
            msg = _("Call disable_sg to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def get_volume(self, volume):
        response = self.sg_ctrl.volumes.get_volume(volume)
        if response['status'] != 0:
            msg = _("Call get_volume to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)
        return response['volume']

    def list_volumes(self):
        response = self.sg_ctrl.volumes.list_volumes()
        if response['status'] != 0:
            msg = _("Call list_volumes to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)
        return response['volumes']

    def attach_volume(self, context, volume, instance_uuid,
                      host_name_sanitized, mountpoint, mode):
        pass

    def detach_volume(self, context, volume, attachment):
        pass

    def initialize_connection(self, context, volume, connector=None):
        driver_data = jsonutils.loads(volume['driver_data'])
        driver_type = driver_data['driver_data']
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
        response = self.sg_ctrl.backups.create_backup(backup)
        if response['status'] != 0:
            msg = _("Call create_backup to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def delete_backup(self, backup):
        response = self.sg_ctrl.backups.delete_backup(backup)
        if response['status'] != 0:
            msg = _("Call delete_backup to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def restore_backup(self, backup, volume, device):
        response = self.sg_ctrl.backups.restore_backup(backup, volume, device)
        if response['status'] != 0:
            msg = _("Call restore_backup to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def get_backup(self, backup):
        response = self.sg_ctrl.backups.get_backup(backup)
        if response['status'] != 0:
            msg = _("Call get_backup to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)
        return response['backup']

    def list_backups(self, volume):
        response = self.sg_ctrl.backups.list_backups(volume)
        if response['status'] != 0:
            msg = _("Call list_backups to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)
        return response['backups']

    def create_snapshot(self, snapshot, volume):
        response = self.sg_ctrl.snapshots.create_snapshot(snapshot, volume)
        if response['status'] != 0:
            msg = _("Call create_snapshot to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def delete_snapshot(self, snapshot):
        response = self.sg_ctrl.snapshots.delete_snapshot(snapshot)
        if response['status'] != 0:
            msg = _("Call delete_snapshot to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def get_snapshot(self, snapshot):
        response = self.sg_ctrl.snapshots.get_snapshot(snapshot)
        if response['status'] != 0:
            msg = _("Call get_snapshot to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)
        return response['snapshot']

    def list_snapshots(self, volume):
        response = self.sg_ctrl.snapshots.list_snapshots(volume)
        if response['status'] != 0:
            msg = _("Call list_snapshots to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)
        return response['snapshots']

    def create_replicate(self, volume):
        response = self.sg_ctrl.replicates.create_replicate(volume)
        if response['status'] != 0:
            msg = _("Call create_replicate to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def enable_replicate(self, volume):
        response = self.sg_ctrl.replicates.enable_replicate(volume)
        if response['status'] != 0:
            msg = _("Call enable_replicate to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def disable_replicate(self, volume):
        response = self.sg_ctrl.replicates.enable_replicate(volume)
        if response['status'] != 0:
            msg = _("Call enable_replicate to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def failover_replicate(self, volume):
        response = self.sg_ctrl.replicates.enable_replicate(volume)
        if response['status'] != 0:
            msg = _("Call enable_replicate to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def delete_replicate(self, volume):
        response = self.sg_ctrl.replicates.enable_replicate(volume)
        if response['status'] != 0:
            msg = _("Call enable_replicate to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def reverse_replicate(self, volume):
        response = self.sg_ctrl.replicates.enable_replicate(volume)
        if response['status'] != 0:
            msg = _("Call enable_replicate to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)
