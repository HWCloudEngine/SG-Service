# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
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
Controller Service
"""

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_service import loopingcall

from sgservice.controller.client_factory import ClientFactory
from sgservice import exception
from sgservice.grpc.client import ControlClient
from sgservice.i18n import _, _LE, _LI
from sgservice import manager
from sgservice import objects
from sgservice.objects import fields

controller_manager_opts = [
    cfg.IntOpt('sync_status_interval',
               default=60,
               help='sync resources status interval'),
]

sg_client_opts = [
    cfg.StrOpt('replication_zone',
               default='nova',
               help='Availability zone of this sg replication and backup '
                    'node.'),
    cfg.StrOpt('sg_client_instance',
               help='The instance id of sg.'),
    cfg.StrOpt('sg_client_host',
               help='The host of sg.'),
    cfg.StrOpt('sg_client_port',
               help='The gprc port of sg'),
    cfg.StrOpt('sg_target_prefix',
               default='iqn.2017-01.huawei.sg:',
               help='Target prefix for sg volumes')
]

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

CONF.register_opts(controller_manager_opts)
CONF.register_opts(sg_client_opts, group='sg_client')


class ControllerManager(manager.Manager):
    """SGService Controller Manager."""

    RPC_API_VERSION = '1.0'

    target = messaging.Target(version=RPC_API_VERSION)

    def __init__(self, service_name=None, *args, **kwargs):
        super(ControllerManager, self).__init__(*args, **kwargs)
        self.sync_status_interval = CONF.sync_status_interval
        self.sg_ctrl = ControlClient(CONF.sg_client.sg_client_host,
                                     CONF.sg_client.sg_client_port)

        self.enable_volumes = {}
        sync_enable_volume_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_enable_volume)
        sync_enable_volume_loop.start(interval=self.sync_status_interval,
                                      initial_delay=self.sync_status_interval)

        self.disable_volumes = {}
        sync_disable_volume_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_disable_volume)
        sync_disable_volume_loop.start(interval=self.sync_status_interval,
                                       initial_delay=self.sync_status_interval)

    def init_host(self, **kwargs):
        """Handle initialization if this is a standalone service"""
        LOG.info(_LI("Starting controller service"))

    def _sync_enable_volume(self):
        for volume_id, volume_info in self.enable_volumes.items():
            cinder_client = volume_info['cinder_client']
            sg_volume = volume_info['sg_volume']
            device = volume_info['device']
            nova_client = volume_info['nova_client']
            try:
                cinder_volume = cinder_client.volumes.get(volume_id)
            except Exception:
                LOG.error(_LE("Sync cinder volume '%s' status failed."),
                          volume_id)
                sg_volume.destroy()
                self.enable_volumes.pop(volume_id)

            if cinder_volume.status == 'in-use':
                LOG.info(_("Attach cinder volume '%s' SG succeed."),
                         volume_id)

                response = self.sg_ctrl.volumes.enable_sg(
                    volume_id, device, cinder_volume.size,
                    self._generate_target_iqn(volume_id))
                if response['status'] != 0:
                    msg = _("Call enable-sg to sg-client failed.")
                    LOG.error(msg)
                    nova_client.volumes.delete_server_volume(
                        CONF.sg_client.sg_client_instance, volume_id)
                    sg_volume.destroy()
                else:
                    msg = _("Call enable-sg to sg-client succeed.")
                    LOG.info(msg)
                    sg_volume.update({'status': fields.VolumeStatus.ENABLED})
                    sg_volume.save()
                self.enable_volumes.pop(volume_id)
            elif cinder_volume.status == 'attaching':
                continue
            else:
                LOG.info(_("Enable cinder volume '%s' SG failed."),
                         volume_id)
                sg_volume.destroy()
                self.enable_volumes.pop(volume_id)

    def _sync_disable_volume(self):
        for volume_id, volume_info in self.disable_volumes.items():
            cinder_client = volume_info['cinder_client']
            nova_client = volume_info['nova_client']
            sg_volume = volume_info['sg_volume']
            try:
                cinder_volume = cinder_client.volumes.get(volume_id)
            except Exception:
                LOG.error(_LE("Sync cinder volume '%s' status failed."),
                          volume_id)
                sg_volume.update({'status': fields.VolumeStatus.ERROR})
                sg_volume.save()
                self.disable_volumes.pop(volume_id)

            if cinder_volume.status == 'available':
                LOG.info(_("Detach cinder volume '%s' from SG succeed."),
                         volume_id)

                response = self.sg_ctrl.volumes.disable_sg(
                    volume_id, self._generate_target_iqn(volume_id))
                if response['status'] != 0:
                    msg = _("Call disable-sg to sg-client failed.")
                    LOG.error(msg)
                    nova_client.volumes.create_server_volume(
                        CONF.sg_client.sg_client_instance, volume_id)
                    sg_volume.update({'status': fields.VolumeStatus.ENABLED})
                    sg_volume.save()
                else:
                    msg = _("Call disable-sg to sg-client succeed.")
                    LOG.error(msg)
                    sg_volume.destroy()
                self.enable_volumes.pop(volume_id)
            elif cinder_volume.status == 'detaching':
                continue
            else:
                LOG.info(_("Disable cinder volume '%s' SG failed."),
                         volume_id)
                if cinder_volume.status == 'in-use':
                    sg_volume.update({'status': fields.VolumeStatus.ENABLED})
                else:
                    sg_volume.update({'status': fields.VolumeStatus.ERROR})
                sg_volume.save()
                self.disable_volumes.pop(volume_id)

    def _generate_target_iqn(self, volume_id):
        return "%s%s" % (CONF.sg_client.sg_target_prefix, volume_id)

    def enable_sg(self, context, volume_id):
        LOG.info(_LI("Enable-SG for this volume with id %s"), volume_id)

        volume = objects.Volume.get_by_id(context, volume_id)
        volume.update({'replication_zone': CONF.sg_client.replication_zone})
        volume.save()

        nova_client = ClientFactory.create_client('nova', context)
        cinder_client = ClientFactory.create_client('cinder', context)

        try:
            volume_attachment = nova_client.volumes.create_server_volume(
                CONF.sg_client.sg_client_instance, volume_id)
        except Exception as err:
            LOG.error(err)
            raise exception.EnableSGFailed(reason=err)

        self.enable_volumes[volume_id] = {
            'device': volume_attachment.device,
            'cinder_client': cinder_client,
            'nova_client': nova_client,
            'sg_volume': volume
        }

    def disable_sg(self, context, volume_id, cascade=False):
        LOG.info(_LI("Disable-SG for this volume with id %s"), volume_id)

        volume = objects.Volume.get_by_id(context, volume_id)
        if cascade:
            LOG.info("Disable SG cascade")
            snapshots = objects.SnapshotList.get_all_by_volume(context,
                                                               volume_id)

            for s in snapshots:
                if s.status != 'deleting':
                    msg = (_("Snapshot '%(id)s' was found in state "
                             "'%(state)s' rather than 'deleting' during "
                             "cascade disable sg") % {'id': s.id,
                                                      'state': s.status})
                    raise exception.InvalidSnapshot(reason=msg)
                self.delete_snapshot(context, s)

            backups = objects.BackupList.get_all_by_volume(context, volume_id)
            for b in backups:
                if b.status != 'deleting':
                    msg = (_("Backup '%(id)s' was found in state "
                             "'%(state)s' rather than 'deleting' during "
                             "cascade disable sg") % {'id': b.id,
                                                      'state': b.status})
                    raise exception.InvalidBackup(reason=msg)
                self.delete_backup(context, b)

        nova_client = ClientFactory.create_client('nova', context)
        cinder_client = ClientFactory.create_client('cinder', context)
        try:
            nova_client.volumes.delete_server_volume(
                CONF.sg_client.sg_client_instance, volume_id)
        except Exception as err:
            LOG.error(err)
            raise exception.EnableSGFailed(reason=err)

        self.disable_volumes[volume_id] = {
            'cinder_client': cinder_client,
            'sg_volume': volume,
            'nova_client': nova_client,
        }