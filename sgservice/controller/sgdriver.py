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

from sgservice.controller.grpc.client import ControlClient
from sgservice import exception
from sgservice.i18n import _

sg_client_opts = [
    cfg.StrOpt('sg_client_host',
               help='The host of sg.'),
    cfg.StrOpt('sg_client_port',
               help='The gprc port of sg'),
    cfg.StrOpt('sg_target_prefix',
               default='iqn.2017-01.huawei.sgs:',
               help='Target prefix for sg volumes')
]

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
CONF.register_opts(sg_client_opts, group='sg_client')


class SGDriver(object):
    def __init__(self):
        self.sg_ctrl = ControlClient(CONF.sg_client.sg_client_host,
                                     CONF.sg_client.sg_client_port)

    def _generate_target_iqn(self, volume_id):
        return "%s%s" % (CONF.sg_client.sg_target_prefix, volume_id)

    def list_devices(self):
        response = self.sg_ctrl.volumes.list_devices()
        if response['status'] != 0:
            msg = _("Call list_devices to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        return response['devices']

    def enable_sg(self, volume_id, device, size):
        response = self.sg_ctrl.volumes.enable_sg(
            volume_id, device, size, self._generate_target_iqn(volume_id))
        if response['status'] != 0:
            msg = _("Call enable_sg to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def disable_sg(self, volume_id):
        response = self.sg_ctrl.volumes.disable_sg(
            volume_id, self._generate_target_iqn(volume_id))
        if response['status'] != 0:
            msg = _("Call disable_sg to sg client failed")
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

    def get_volume(self, volume_id):
        response = self.sg_ctrl.volumes.get_volume(volume_id)
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
        # iscsi mode
        driver_volume_type = "iscsi"
        target_portal = "162.3.117.150:3260"
        target_iqn = self._generate_target_iqn(volume.id)
        target_lun = 1
        data = {
            "target_discovered": False,
            "target_portal": target_portal,
            "target_iqn": target_iqn,
            "target_lun": target_lun,
            "volume_id": volume.id,
            "display_name": volume.display_name,
        }
        connection_info = {'driver_volume_type': driver_volume_type,
                           'data': data}
        return connection_info

    def create_backup(self, **kwargs):
        pass

    def delete_backup(self, **kwargs):
        pass

    def restore_backup(self, **kwargs):
        pass

    def create_snapshot(self, **kwargs):
        pass

    def delete_snapshot(self, **kwargs):
        pass

    def enable_replicate(self, **kwargs):
        pass

    def disable_replicate(self, **kwarg):
        pass

    def failover_replicate(self, **kwargs):
        pass

    def delete_replicate(self, **kwargs):
        pass

    def reverse_replicate(self, **kwargs):
        pass
