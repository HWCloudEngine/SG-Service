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

"""
Client side of the controller manager RPC API.
"""

from oslo_config import cfg
import oslo_messaging as messaging

from sgservice.objects import base as objects_base
from sgservice import rpc

CONF = cfg.CONF


class ControllerAPI(object):
    """Client side of the controller rpc API.

    API version history:

        1.0 - Initial version.
    """

    RPC_API_VERSION = '1.0'

    def __init__(self):
        super(ControllerAPI, self).__init__()
        target = messaging.Target(topic=CONF.controller_topic,
                                  version=self.RPC_API_VERSION)
        serializer = objects_base.SGServiceObjectSerializer()
        self.client = rpc.get_client(target, version_cap=None,
                                     serializer=serializer)

    def enable_sg(self, ctxt, volume):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'enable_sg', volume_id=volume.id)

    def disable_sg(self, ctxt, volume, cascade=False):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'disable_sg', volume_id=volume.id,
                          cascade=cascade)

    def attach_volume(self, ctxt, volume, instance_uuid, host_name,
                      mountpoint, mode):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'attach_volume', volume_id=volume.id,
                          instance_uuid=instance_uuid, host_name=host_name,
                          mountpoint=mountpoint, mode=mode)

    def detach_volume(self, ctxt, volume, attachment_id):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'detach_volume', volume_id=volume.id,
                          attachment_id=attachment_id)

    def initialize_connection(self, ctxt, volume, connector):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.call(ctxt, 'initialize_connection', volume_id=volume.id,
                          connector=connector)

    def create_backup(self, ctxt, backup):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'create_backup', backup_id=backup.id)

    def delete_backup(self, ctxt, backup):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'delete_backup', backup_id=backup.id)

    def restore_backup(self, ctxt, backup, volume):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'restore_backup', backup_id=backup.id,
                          volume_id=volume.id)

    def create_snapshot(self, ctxt, snapshot, volume):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'create_snapshot', snapshot_id=snapshot.id,
                          volume_id=volume.id)

    def delete_snapshot(self, ctxt, snapshot):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'delete_snapshot', snapshot_id=snapshot.id)

    def rollback_snapshot(self, ctxt, snapshot, volume):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'rollback_snapshot', snapshot_id=snapshot.id,
                          volume_id=volume.id)

    def create_replicate(self, ctxt, volume):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'create_replicate', volume_id=volume.id)

    def enable_replicate(self, ctxt, volume):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'enable_replicate', volume_id=volume.id)

    def disable_replicate(self, ctxt, volume):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'disable_replicate', volume_id=volume.id)

    def delete_replicate(self, ctxt, volume):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'delete_replicate', volume_id=volume.id)

    def failover_replicate(self, ctxt, volume, force=False):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'failover_replicate', volume_id=volume.id, force=force)

    def reverse_replicate(self, ctxt, volume):
        cctxt = self.client.prepare(version='1.0')
        return cctxt.cast(ctxt, 'reverse_replicate', volume_id=volume.id)
