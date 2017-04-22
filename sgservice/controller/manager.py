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

from eventlet import greenthread
import six

from cinderclient import exceptions as cinder_exc
from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_serialization import jsonutils
from oslo_service import loopingcall
from oslo_utils import excutils
from oslo_utils import importutils
from oslo_utils import uuidutils

from sgservice.common import constants
from sgservice.common.clients import cinder
from sgservice.common.clients import nova
from sgservice import context as sg_context
from sgservice import exception
from sgservice.i18n import _, _LE, _LI
from sgservice import manager
from sgservice import objects
from sgservice.objects import fields
from sgservice import utils

controller_manager_opts = [
    cfg.IntOpt('sync_status_interval',
               default=60,
               help='sync resources status interval'),
    cfg.StrOpt('sg_driver',
               default='sgservice.controller.drivers.iscsi.ISCSISGDriver',
               help='The class name of storage gateway driver'),
    cfg.IntOpt('retry_attempts',
               default=10)
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

SUCCEED = 'succeed'
FAILED = 'failed'

BACKUP_STATUS_ACTION = {
    fields.BackupStatus.CREATING: 'create',
    fields.BackupStatus.DELETING: 'delete',
    fields.BackupStatus.RESTORING: 'restore'
}

SNAPSHOT_STATUS_ACTION = {
    fields.SnapshotStatus.CREATING: 'create',
    fields.SnapshotStatus.DELETING: 'delete',
}

REPLICATE_STATUS_ACTION = {
    fields.ReplicateStatus.ENABLING: 'enable_replicate',
    fields.ReplicateStatus.DISABLING: 'disable_replicate',
    fields.ReplicateStatus.DELETING: 'delete_replicate',
    fields.ReplicateStatus.FAILING_OVER: 'failover_replicate',
    fields.ReplicateStatus.REVERSING: 'reverse_replicate'
}


class ControllerManager(manager.Manager):
    """SGService Controller Manager."""

    RPC_API_VERSION = '1.0'
    target = messaging.Target(version=RPC_API_VERSION)

    def __init__(self, service_name=None, *args, **kwargs):
        super(ControllerManager, self).__init__(*args, **kwargs)
        self.sync_status_interval = CONF.sync_status_interval
        driver_class = CONF.sg_driver
        self.driver = importutils.import_object(driver_class)
        self.admin_context = sg_context.get_admin_context()

        self.sync_attach_volumes = {}  # used to sync enable-volumes from cinder
        sync_attach_volume_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_attach_volumes)
        sync_attach_volume_loop.start(interval=self.sync_status_interval,
                                      initial_delay=self.sync_status_interval)

        self.sync_detach_volumes = {}  # used to sync disable-volumes from cinder
        sync_detach_volume_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_detach_volumes)
        sync_detach_volume_loop.start(interval=self.sync_status_interval,
                                      initial_delay=self.sync_status_interval)

        self.sync_backups = {}  # used to sync backups from sg-client
        sync_backups_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_backups)
        sync_backups_loop.start(interval=self.sync_status_interval,
                                initial_delay=self.sync_status_interval)

        self.sync_snapshots = {}  # used to sync snapshots from sg-client
        sync_snapshots_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_snapshots)
        sync_snapshots_loop.start(interval=self.sync_status_interval,
                                  initial_delay=self.sync_status_interval)

        self.sync_replicates = {}  # used to sync volumes from sg-client
        sync_replicates_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_replicates)
        sync_replicates_loop.start(interval=self.sync_status_interval,
                                   initial_delay=self.sync_status_interval)

    def init_host(self, **kwargs):
        """Handle initialization if this is a standalone service"""
        LOG.info(_LI("Starting controller service"))
        self._init_volumes(self.admin_context)
        self._init_backups(self.admin_context)
        self._init_replicates(self.admin_context)
        self._init_snapshots(self.admin_context)

    def _init_volumes(self, context):
        volumes = objects.VolumeList.get_all(context)
        for volume in volumes:
            if volume.status == fields.VolumeStatus.ENABLING:
                self._init_enabling_volume(context, volume)
            elif volume.status in [fields.VolumeStatus.DISABLING,
                                   fields.VolumeStatus.DELETING]:
                self._init_deleting_volume(context, volume)
            elif volume.status == fields.VolumeStatus.CREATING:
                self._init_creating_volume(context, volume)

    def _init_enabling_volume(self, context, volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id  = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            if (cinder_volume.status == 'in-use'
                and len(cinder_volume.attachments) == 1
                and cinder_volume.attachments[0]['server_id']
                    == CONF.sg_client.sg_client_instance):
                mountpoint = cinder.attachments[0]['device']
                self._do_enable_sg(SUCCEED, volume, mountpoint)
            elif cinder_volume.status in ['available', 'attaching']:
                if cinder_volume.status == 'available':
                    self._attach_volume_to_sg(context, logical_volume_id)
                self.sync_attach_volumes[logical_volume_id] = {
                    "cinder_client": cinder_client,
                    'function': self._do_enable_sg,
                    'kwargs': {'volume': volume}
                }
            else:
                self._do_enable_sg(FAILED, volume)
        except Exception:
            self._do_enable_sg(FAILED, volume)

    def _init_deleting_volume(self, context, volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            if (cinder_volume.status in ['in-use', 'detaching']
                and len(cinder_volume.attachments) == 1
                and cinder_volume.attachments[0]['server_id']
                    == CONF.sg_client.sg_client_instance):
                if cinder_volume.status == 'in-use':
                    driver_volume = self.driver.get(volume)
                    LOG.info(_LI('driver_volume info:%s'), driver_volume)
                    if driver_volume['status'] != fields.VolumeStatus.DELETED:
                        self.driver.disable_sg(volume)
                    self._detach_volume_from_sg(context, logical_volume_id)
                self.sync_detach_volumes[logical_volume_id] = {
                    'cinder_client': cinder_client,
                    'function': self._finish_delete_volume,
                    'kwargs': {'volume': volume}
                }
            else:
                self._finish_delete_volume(SUCCEED, volume)
        except cinder_exc.NotFound:
            self._finish_delete_volume(SUCCEED, volume)
        except Exception:
            self._finish_delete_volume(FAILED, volume)

    def _init_creating_volume(self, context, volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        snapshot = objects.Snapshot.get_by_id(context, volume.snapshot_id)
        try:
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            retry = CONF.retry_attempts
            while cinder_volume.status == 'creating' and retry != 0:
                greenthread.sleep(CONF.sync_status_interval)
                try:
                    cinder_volume = cinder_client.volumes.get(
                        logical_volume_id)
                except Exception:
                    msg = (_("Get the volume '%s' from cinder failed."),
                           logical_volume_id)
                    LOG.error(msg)
                    self._finish_create_volume(FAILED, volume, snapshot)
                    return
                retry -= 1
            if retry == 0:
                msg = (_("Wait cinder volume %s to be available timeout."),
                       logical_volume_id)
                LOG.error(msg)
                self._finish_create_volume(FAILED, volume, snapshot)
                return
            if (cinder_volume.status == 'in-use'
                and len(cinder_volume.attachments) == 1
                and cinder_volume.attachments[0]['server_id']
                    == CONF.sg_client.sg_client_instance):
                mountpoint = cinder_volume.attachments[0]['device']
                self._do_create_volume(SUCCEED, context, mountpoint, snapshot,
                                       volume)
            elif cinder_volume.status in ['available', 'attaching']:
                if cinder_volume.status == 'available':
                    self._attach_volume_to_sg(context, logical_volume_id)
                self.sync_attach_volumes[logical_volume_id] = {
                    "cinder_client": cinder_client,
                    'function': self._do_create_volume,
                    'kwargs': {'snapshot': snapshot,
                               'volume': volume,
                               'context': context}
                }
            elif (cinder_volume.status == 'detaching'
                  and len(cinder_volume.attachments) == 1
                  and cinder_volume.attachments[0]['server_id']
                    == CONF.sg_client.sg_client_instance):
                self.sync_detach_volumes[logical_volume_id] = {
                    'cinder_client': cinder_client,
                    'function': self._finish_create_volume,
                    'kwargs': {'volume': volume}
                }
            else:
                self._finish_create_volume(FAILED, volume, snapshot)
        except Exception:
            self._finish_create_volume(FAILED, volume, snapshot)

    def _init_restoring_volume(self, context, volume, backup):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            if (cinder_volume.status == 'in-use'
                and len(cinder_volume.attachments) == 1
                and cinder_volume.attachments[0]['server_id']
                    == CONF.sg_client.sg_client_instance):
                mountpoint = cinder_volume.attachments[0]['device']
                self._do_restore_backup(SUCCEED, context, backup,
                                        volume, mountpoint)
            elif cinder_volume.status in ['available', 'attaching']:
                if cinder_volume.status == 'available':
                    self._attach_volume_to_sg(context, logical_volume_id)
                self.sync_attach_volumes[logical_volume_id] = {
                    "cinder_client": cinder_client,
                    'function': self._do_restore_backup,
                    'kwargs': {'backup': backup,
                               'volume': volume,
                               'context': context}
                }
            elif (cinder_volume.status == 'detaching'
                  and len(cinder_volume.attachments) == 1
                  and cinder_volume.attachments[0]['server_id']
                  == CONF.sg_client.sg_client_instance):
                self.sync_detach_volumes[logical_volume_id] = {
                    'cinder_client': cinder_client,
                    'function': self._finish_restore_backup,
                    'kwargs': {'volume': volume,
                               'backup': backup}
                }
            else:
                self._finish_restore_backup(FAILED, backup, volume)
        except Exception:
            self._finish_restore_backup(FAILED, backup, volume)

    def _init_replicates(self, context):
        volumes = objects.VolumeList.get_all(context)
        for volume in volumes:
            if volume.replicate_status in REPLICATE_STATUS_ACTION.keys():
                self.sync_replicates[volume.id] = {
                    'volume': volume,
                    'action': REPLICATE_STATUS_ACTION[volume.replicate_status]
                }

    def _init_backups(self, context):
        backups = objects.BackupList.get_all(context)
        for backup in backups:
            if backup.status in BACKUP_STATUS_ACTION.keys():
                try:
                    volume = objects.Volume.get_by_id(context,
                                                      backup['volume_id'])
                except Exception:
                    volume = None
                if backup.status == fields.BackupStatus.RESTORING:
                    self._init_restoring_volume(context, volume, backup)
                else:
                    self.sync_backups[backup.id] = {
                        'backup': backup,
                        'action': BACKUP_STATUS_ACTION[backup.status],
                        'volume': volume
                    }

    def _init_snapshots(self, context):
        snapshots = objects.SnapshotList.get_all(context)
        for snapshot in snapshots:
            if snapshot.status in SNAPSHOT_STATUS_ACTION.keys():
                volume = objects.Volume.get_by_id(context,
                                                  snapshot['volume_id'])
                self.sync_snapshots[snapshot.id] = {
                    'snapshot': snapshot,
                    'volume': volume,
                    'action': SNAPSHOT_STATUS_ACTION[snapshot.status]
                }

    def _create_cinder_client(self, context):
        if context.is_admin:
            return cinder.get_admin_client()
        else:
            return cinder.get_project_context_client(context)

    def _create_nova_client(self, context):
        if context.is_admin:
            return nova.get_admin_client()
        else:
            return nova.get_project_context_client(context)

    def _attach_volume_to_sg(self, context, volume_id):
        """ Attach volume to sg-client and get mountpoint
        """
        try:
            nova_client = self._create_nova_client(context)
            volume_attachment = nova_client.volumes.create_server_volume(
                CONF.sg_client.sg_client_instance, volume_id)
            return volume_attachment.device
        except Exception as err:
            LOG.error(err)
            raise exception.AttachSGFailed(reason=err)

    def _sync_backups(self):
        for backup_id, backup_info in self.sync_backups.items():
            action = backup_info['action']
            try:
                backup = objects.Backup.get_by_id(self.admin_context,
                                                  backup_id)
            except Exception:
                self.sync_backups.pop(backup_id)
                continue
            if backup.status not in BACKUP_STATUS_ACTION.keys():
                self.sync_backups.pop(backup_id)
                continue
            try:
                driver_backup = self.driver.get_backup(backup)
                LOG.info(_LE("Driver backup info: %s"), driver_backup)
            except exception.SGDriverError as e:
                LOG.error(e)
                continue
            if driver_backup['status'] in BACKUP_STATUS_ACTION.keys():
                continue
            if action == 'delete':
                if driver_backup['status'] == fields.BackupStatus.DELETED:
                    self._finish_delete_backup(SUCCEED, backup)
                else:
                    self._finish_delete_backup(FAILED, backup,
                                               driver_backup['status'])
            elif action == 'create':
                volume = backup_info['volume']
                if driver_backup['status'] == fields.BackupStatus.AVAILABLE:
                    self._finish_create_backup(SUCCEED, volume, backup)
                else:
                    self._finish_create_backup(FAILED, volume, backup)
            elif action == 'restore':
                # TODO(luobin)
                pass
            self.sync_backups.pop(backup_id)

    def _sync_snapshots(self):
        for snapshot_id, snapshot_info in self.sync_snapshots.items():
            action = snapshot_info['action']
            try:
                snapshot = objects.Snapshot.get_by_id(self.admin_context,
                                                      snapshot_id)
            except Exception:
                self.sync_snapshots.pop(snapshot_id)
                continue
            if snapshot.status not in SNAPSHOT_STATUS_ACTION.keys():
                self.sync_snapshots.pop(snapshot_id)
                continue

            try:
                driver_snapshot = self.driver.get_snapshot(snapshot)
                LOG.info(_LI("Driver snapshot info: %s"), driver_snapshot)
            except exception.SGDriverError as e:
                LOG.error(e)
                continue
            if driver_snapshot['status'] in SNAPSHOT_STATUS_ACTION.keys():
                continue
            if action == 'delete':
                if driver_snapshot['status'] == fields.SnapshotStatus.DELETED:
                    self._finish_delete_snapshot(SUCCEED, snapshot)
                else:
                    self._finish_delete_snapshot(FAILED, snapshot,
                                                 driver_snapshot['status'])
            elif action == 'create':
                if (driver_snapshot['status']
                        == fields.SnapshotStatus.AVAILABLE):
                    self._finish_create_snapshot(SUCCEED, snapshot)
                else:
                    self._finish_create_snapshot(FAILED, snapshot)
            elif action == 'rollback':
                volume = snapshot_info['volume']
                if driver_snapshot['status'] == fields.SnapshotStatus.DELETED:
                    self._finish_rollback_snapshot(SUCCEED, snapshot, volume)
                else:
                    self._finish_rollback_snapshot(FAILED, snapshot, volume,
                                                   driver_snapshot['status'])
            self.sync_snapshots.pop(snapshot_id)

    def _sync_replicates(self):
        for volume_id, volume_info in self.sync_replicates.items():
            action = volume_info['action']
            try:
                volume = objects.Volume.get_by_id(self.admin_context,
                                                  volume_id)
            except Exception:
                self.sync_replicates.pop(volume_id)
                continue
            if volume.replicate_status not in REPLICATE_STATUS_ACTION.keys():
                self.sync_replicates.pop(volume_id)
                continue
            try:
                driver_volume = self.driver.get_volume(volume)
                LOG.info(_LI("Driver volume info: %s"), driver_volume)
            except exception.SGDriverError as e:
                LOG.error(e)
                continue
            if (driver_volume['replicate_status']
                    in REPLICATE_STATUS_ACTION.keys()):
                continue
            replicate_status = driver_volume['replicate_status']
            if action in ['create_replicate', 'enable_replicate']:
                if replicate_status == fields.ReplicateStatus.ENABLED:
                    self._finish_enable_replicate(SUCCEED, volume)
                else:
                    self._finish_enable_replicate(FAILED, volume,
                                                  replicate_status)
            elif action == 'delete_replicate':
                if replicate_status in [None, fields.ReplicateStatus.DELETED]:
                    self._finish_delete_replicate(SUCCEED, volume)
                else:
                    self._finish_delete_replicate(FAILED, volume,
                                                  replicate_status)
            elif action == 'failover_replicate':
                if replicate_status == fields.ReplicateStatus.FAILED_OVER:
                    self._finish_failover_replicate(SUCCEED, volume)
                else:
                    self._finish_failover_replicate(FAILED, volume,
                                                    replicate_status)
            elif action == 'disable_replicate':
                if replicate_status == fields.ReplicateStatus.DISABLED:
                    self._finish_disable_replicate(SUCCEED, volume)
                else:
                    self._finish_disable_replicate(FAILED, volume,
                                                   replicate_status)
            elif action == 'reverse_replicate':
                if replicate_status == fields.ReplicateStatus.DISABLED:
                    self._finish_reverse_replicate(SUCCEED, volume,
                                                   driver_volume)
                else:
                    self._finish_reverse_replicate(FAILED, volume,
                                                   driver_volume)
            self.sync_replicates.pop(volume_id)

    def _get_attach_device(self, mountpoint):
        try:
            devices = self.driver.list_devices()
            device = [d for d in devices if d[-1] == mountpoint[-1]]
            if len(device) == 0:
                msg = _("Get volume device failed")
                LOG.error(msg)
                raise exception.AttachSGFailed(reason=msg)
            elif len(device) > 1:
                device = [d for d in devices if d[-2] == mountpoint[-2]]
                if len(device) == 0:
                    msg = _("Get volume device failed")
                    LOG.error(msg)
                    raise exception.AttachSGFailed(reason=msg)
            return device[0]
        except exception.SGDriverError as err:
            msg = (_("call list-devices to sg-client failed, err: %s."), err)
            LOG.error(msg)
            raise err

    def _detach_volume_from_sg(self, context, volume_id):
        # detach volume from sg-client
        try:
            nova_client = self._create_nova_client(context)
            nova_client.volumes.delete_server_volume(
                CONF.sg_client.sg_client_instance, volume_id)
        except Exception as err:
            LOG.error(err)
            raise exception.DetachSGFailed(reason=err)

    def _sync_attach_volumes(self):
        for volume_id, volume_info in self.sync_attach_volumes.items():
            cinder_client = volume_info['cinder_client']
            function = volume_info['function']
            kwargs = volume_info['kwargs']

            try:
                cinder_volume = cinder_client.volumes.get(volume_id)
                if cinder_volume.status == 'attaching':
                    continue
                elif (cinder_volume.status == 'in-use'
                      and len(cinder_volume.attachments) == 1
                      and cinder_volume.attachments[0]['server_id']
                        == CONF.sg_client.sg_client_instance):
                    mountpoint = cinder_volume.attachments[0]['device']
                    kwargs['mountpoint'] = mountpoint
                    function(SUCCEED, **kwargs)
                else:
                    function(FAILED, **kwargs)
                self.sync_attach_volumes.pop(volume_id)
            except cinder_exc.NotFound:
                LOG.error(_LE("Sync cinder volume '%s' not found."), volume_id)
                function(FAILED, **kwargs)
                self.sync_attach_volumes.pop(volume_id)
            except Exception:
                LOG.error(_LE("Sync cinder volume '%s' status failed."),
                          volume_id)

    def _sync_detach_volumes(self):
        for volume_id, volume_info in self.sync_detach_volumes.items():
            cinder_client = volume_info['cinder_client']
            function = volume_info['function']
            kwargs = volume_info['kwargs']

            try:
                cinder_volume = cinder_client.volumes.get(volume_id)
                if cinder_volume.status == 'available':
                    LOG.info(_("Detach cinder volume '%s' from SG succeed."),
                             volume_id)
                    function('succeed', **kwargs)
                elif cinder_volume.status == 'detaching':
                    continue
                else:
                    LOG.info(_("Detach cinder volume '%s' from SG failed."),
                             volume_id)
                    function('failed', **kwargs)
                self.sync_detach_volumes.pop(volume_id)
            except cinder_exc.NotFound:
                LOG.error(_LE("Sync cinder volume '%s' not found."), volume_id)
                function(volume_id, 'failed')
                self.sync_detach_volumes.pop(volume_id)
            except Exception:
                LOG.error(_LE("Sync cinder volume '%s' status failed."),
                          volume_id)

    def _do_enable_sg(self, sync_result, volume, mountpoint=None):
        if sync_result == 'succeed':
            try:
                device = self._get_attach_device(mountpoint)
                driver_data = self.driver.enable_sg(volume, device)
                volume.update(
                    {'status': fields.VolumeStatus.ENABLED,
                     'driver_data': jsonutils.dumps(driver_data)})
                volume.save()
                LOG.info(_LI('enable sg succeed, volume_id:%s'), volume['id'])
            except Exception:
                LOG.error(_LE('enable sg failed, volume_id: %s'), volume['id'])
                volume.update({'status': fields.VolumeStatus.ERROR})
                volume.save()
        else:
            LOG.error(_LE('enable sg failed, volume_id: %s'), volume['id'])
            volume.update({'status': fields.VolumeStatus.ERROR})
            volume.save()

    def _finish_delete_volume(self, sync_result, volume):
        if sync_result == 'succeed':
            LOG.info(_LI('Delete or disable sg volume succeed, volume_id:%s'),
                     volume['id'])
            volume.destroy()
        else:
            LOG.error(_LE('Delete or disable sg volume failed, volume_id: %s'),
                      volume['id'])
            volume.update({'status': fields.VolumeStatus.ERROR})
            volume.save()

    def delete(self, context, volume_id):
        LOG.info(_LI("Delete sg volume with id %s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume_id

        try:
            cinder_client = self._create_cinder_client(context)
            c_volume = cinder_client.volumes.get(logical_volume_id)
            if (c_volume.status == 'in-use'
                and len(c_volume.attachments) == 1
                and c_volume.attachments[0]['server_id']
                    == CONF.sg_client.sg_client_instance):
                self._detach_volume_from_sg(context, logical_volume_id)
                self.sync_detach_volumes[logical_volume_id] = {
                    'cinder_client': cinder_client,
                    'function': self._finish_delete_volume,
                    'kwargs': {'volume': volume}
                }
            else:
                self._finish_delete_volume(SUCCEED, volume)
        except cinder_exc.NotFound:
            self._finish_delete_volume(SUCCEED, volume)
            return
        except Exception as err:
            msg = (_LE('delete sg volume:%(volume_id) failed, err:%(err)s'),
                   {'volume_id': volume.id, 'err': err})
            self._finish_delete_volume(FAILED, volume)
            raise exception.CinderClientError(reason=msg)

    def enable_sg(self, context, volume_id):
        LOG.info(_LI("Enable-SG for this volume with id %s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)
        volume.update({'replication_zone': CONF.sg_client.replication_zone})
        volume.save()

        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume_id
        try:
            self._attach_volume_to_sg(context, logical_volume_id)
            cinder_client = self._create_cinder_client(context)
            self.sync_attach_volumes[logical_volume_id] = {
                'cinder_client': cinder_client,
                'function': self._do_enable_sg,
                'kwargs': {'volume': volume}
            }
        except Exception as err:
            msg = (_LE('enable sg volume:%(volume_id) failed, err:%(err)s'),
                   {'volume_id': volume.id, 'err': err})
            self._do_enable_sg(FAILED, volume)
            raise exception.EnableSGFailed(reason=msg)

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

        try:
            self.driver.disable_sg(volume)
            LOG.info('call disable-sg to sg-client succeed, volume_id: %s' %
                     volume_id)
        except exception.SGDriverError as err:
            LOG.info('call disable-sg to sg-client failed, volume_id: %s' %
                     volume_id)
            volume.update({'status': fields.VolumeStatus.ENABLED})
            volume.save()
            raise exception.DisableSGFailed(reason=err)

        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume_id
        try:
            self._detach_volume_from_sg(context, logical_volume_id)
            cinder_client = self._create_cinder_client(context)
            self.sync_detach_volumes[logical_volume_id] = {
                'cinder_client': cinder_client,
                'function': self._finish_delete_volume,
                'kwargs': {'volume': volume}
            }
        except Exception as err:
            LOG.error(err)
            self._finish_delete_volume(FAILED, volume)
            raise exception.DisableSGFailed(reason=err)

    def attach_volume(self, context, volume_id, instance_uuid, host_name,
                      mountpoint, mode):
        LOG.info(_LI("Attach volume '%s' to '%s'"), volume_id, instance_uuid)

        volume = objects.Volume.get_by_id(context, volume_id)
        if volume['status'] == 'attaching':
            access_mode = volume['access_mode']
            if access_mode is not None and access_mode != mode:
                LOG.error(_('being attached by different mode'))
                raise exception.InvalidVolumeAttachMode(mode=mode,
                                                        volume_id=volume.id)

        host_name_sanitized = utils.sanitize_hostname(
            host_name) if host_name else None
        if instance_uuid:
            attachments = self.db.volume_attachment_get_all_by_instance_uuid(
                context, volume_id, instance_uuid)
        else:
            attachments = self.db.volume_attachment_get_all_by_host(
                context, volume_id, host_name_sanitized)
        if attachments:
            self.db.volume_update(context, volume_id, {'status': 'in-use'})
            return

        values = {'volume_id': volume_id,
                  'attach_status': 'attaching'}
        attachment = self.db.volume_attach(context.elevated(), values)
        attachment_id = attachment['id']

        if instance_uuid and not uuidutils.is_uuid_like(instance_uuid):
            self.db.volume_attachment_update(
                context, attachment_id, {'attach_status': 'error_attaching'})
            raise exception.InvalidUUID(uuid=instance_uuid)

        try:
            self.driver.attach_volume(context, volume, instance_uuid,
                                      host_name_sanitized, mountpoint, mode)
        except Exception as err:
            self.db.volume_attachment_update(
                context, attachment_id, {'attach_status': 'error_attaching'})
            raise err

        self.db.volume_attached(context.elevated(),
                                attachment_id,
                                instance_uuid,
                                host_name_sanitized,
                                mountpoint,
                                mode)
        LOG.info(_LI("Attach volume completed successfully."))
        return self.db.volume_attachment_get(context, attachment_id)

    def detach_volume(self, context, volume_id, attachment_id):
        LOG.info(_LI("Detach volume with id:'%s'"), volume_id)

        volume = self.db.volume_get(context, volume_id)
        if attachment_id:
            try:
                attachment = self.db.volume_attachment_get(context,
                                                           attachment_id)
            except exception.VolumeAttachmentNotFound:
                LOG.info(_LI("Volume detach called, but volume not attached"))
                self.db.volume_detached(context, volume_id, attachment_id)
                return
        else:
            attachments = self.db.volume_attachment_get_all_by_volume_id(
                context, volume_id)
            if len(attachments) > 1:
                msg = _("Detach volume failed: More than one attachment, "
                        "but no attachment_id provide.")
                LOG.error(msg)
                raise exception.InvalidVolume(reason=msg)
            elif len(attachments) == 1:
                attachment = attachments[0]
            else:
                LOG.info(_LI("Volume detach called, but volume not attached"))
                self.db.volume_update(context, volume_id,
                                      {'status': fields.VolumeStatus.ENABLED})
                return

        try:
            self.driver.detach_volume(context, volume, attachment)
        except Exception as err:
            self.db.volume_attachment_update(
                context, attachment_id, {'attach_status': 'error_detaching'})
            raise err

        self.db.volume_detached(context.elevated(), volume_id,
                                attachment.get('id'))
        LOG.info(_LI("Detach volume completed successfully."))

    def initialize_connection(self, context, volume_id, connector):
        LOG.info(_LI("Initialize volume connection with id'%s'"), volume_id)

        volume = objects.Volume.get_by_id(context, volume_id)
        try:
            conn_info = self.driver.initialize_connection(context, volume,
                                                          connector)
        except Exception as err:
            msg = (_('Driver initialize connection failed '
                     '(error: %(err)s).') % {'err': six.text_type(err)})
            LOG.error(msg)
            raise exception.SGDriverError(reason=msg)

        LOG.info(_LI("Initialize connection completed successfully."))
        return conn_info

    def _update_backup_error(self, backup):
        backup.update({'status': fields.BackupStatus.ERROR})
        backup.save()

    def _finish_create_backup(self, result, volume, backup):
        if result == SUCCEED:
            LOG.info(_LI("Create backup:%s succeed."), backup.id)
            backup.update({'status': fields.BackupStatus.AVAILABLE})
            backup.save()
            volume.update({'status': volume.previous_status})
            volume.save()
        else:
            LOG.info(_LI("Create backup:%s failed."), backup.id)
            self._update_backup_error(backup)
            volume.update({'status': volume.previous_status,
                           'previous_status': 'error_backing_up'})
            volume.save()

    def create_backup(self, context, backup_id):
        backup = objects.Backup.get_by_id(context, backup_id)
        volume_id = backup.volume_id
        LOG.info(_LI("Create backup started, backup:%(backup_id)s, volume: "
                     "%(volume_id)s"),
                 {'volume_id': volume_id, 'backup_id': backup_id})
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = 'backing-up'
        actual_status = volume['status']
        if actual_status != expected_status:
            msg = (_('Create backup aborted, expected volume status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._finish_create_backup(FAILED, volume, backup)
            raise exception.InvalidVolume(reason=msg)

        expected_status = fields.BackupStatus.CREATING
        actual_status = backup['status']
        if actual_status != expected_status:
            msg = (_('Create backup aborted, expected backup status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._finish_create_backup(FAILED, volume, backup)
            raise exception.InvalidBackup(reason=msg)

        try:
            self.driver.create_backup(backup=backup)
            driver_data = {'volume_id': volume_id,
                           'backup_id': backup_id}
            backup.update({'driver_data': jsonutils.dumps(driver_data)})
            backup.save()
            self.sync_backups[backup_id] = {
                'action': 'create',
                'backup': backup,
                'volume': volume
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("create backup:%(backup_id)s failed, "
                              "err:%(err)s"),
                          {"backup_id": backup_id, "err": err})
                self._finish_create_backup(FAILED, volume, backup)

    def _finish_delete_backup(self, result, backup, status=None):
        if result == SUCCEED:
            LOG.info(_LI("Delete backup:%s succeed."), backup.id)
            if backup.parent_id:
                parent_backup = objects.Backup.get_by_id(
                    self.admin_context,
                    backup.parent_id)
                if parent_backup.has_dependent_backups:
                    parent_backup.num_dependent_backups -= 1
                parent_backup.save()
            backup.destroy()
        else:
            status = status if status else fields.BackupStatus.ERROR
            backup.update({'status': status})
            backup.save()

    def delete_backup(self, context, backup_id):
        LOG.info(_LI("Delete backup started, backup:%s"), backup_id)
        backup = objects.Backup.get_by_id(context, backup_id)

        expected_status = fields.BackupStatus.DELETING
        actual_status = backup['status']
        if actual_status != expected_status:
            msg = (_('Delete backup aborted, expected backup status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_delete_backup(FAILED, backup)
            raise exception.InvalidBackup(reason=msg)

        try:
            if backup.volume_id is None:
                self._finish_delete_backup(SUCCEED, backup)
                return
            driver_backup = self.driver.get_backup(backup)
            if driver_backup['status'] == fields.BackupStatus.DELETED:
                self._finish_delete_backup(SUCCEED, backup)
                return
            self.driver.delete_backup(backup=backup)
            self.sync_backups[backup_id] = {
                'action': 'delete',
                'backup': backup
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE('delete backup:%(backup_id)s failed, '
                              'err:%(err)s'),
                          {"backup_id": backup_id, "err": err})
                self._finish_delete_backup(FAILED, backup)

    def _do_restore_backup(self, sync_result, context, backup, volume,
                           mountpoint):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        if sync_result == SUCCEED:
            try:
                cinder_client = self._create_cinder_client(context)
                device = self._get_attach_device(mountpoint)
                # TODO(luobin): current restore is a synchronization function
                self.driver.restore_backup(backup, volume, device)
                self._detach_volume_from_sg(context, logical_volume_id)
                self.sync_detach_volumes[logical_volume_id] = {
                    'cinder_client': cinder_client,
                    'function': self._finish_restore_backup,
                    'kwargs': {'volume': volume, 'backup': backup}
                }
            except Exception as err:
                msg = (_LE("Restore backup:%(backup_id)s failed, "
                           "err:%(err)s."),
                       {"backup_id": backup.id, "err": err})
                LOG.error(msg)
                self._finish_restore_backup(FAILED, backup, volume)
        else:
            msg = (_LE("Restore backup:%s failed, err: "
                       "attach volume to sg-client failed."), backup.id)
            LOG.error(msg)
            self._finish_restore_backup(FAILED, backup, volume)

    def _finish_restore_backup(self, result, backup, volume):
        if result == SUCCEED:
            LOG.error(_LE('restore backup succeed, backup_id: %s'),
                      backup['id'])
            backup.update({'status': fields.BackupStatus.AVAILABLE})
            backup.save()
            volume.update({'status': fields.VolumeStatus.AVAILABLE})
            volume.save()
        else:
            backup.update({'status': fields.BackupStatus.AVAILABLE})
            backup.save()
            volume.update({'status': fields.VolumeStatus.ERROR_RESTORING})
            volume.save()

    def restore_backup(self, context, backup_id, volume_id):
        LOG.info(_LI("Restore backup started, backup:%(backup_id)s, volume: "
                     "%(volume_id)s"),
                 {'volume_id': volume_id, 'backup_id': backup_id})
        backup = objects.Backup.get_by_id(context, backup_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = 'restoring'
        actual_status = backup['status']
        if actual_status != expected_status:
            msg = (_LE('Restore backup aborted, expected backup status '
                       '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_restore_backup(FAILED, backup, volume)
            raise exception.InvalidBackup(reason=msg)

        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume_id
        try:
            self._attach_volume_to_sg(context, logical_volume_id)
            cinder_client = self._create_cinder_client(context)
            self.sync_attach_volumes[logical_volume_id] = {
                'cinder_client': cinder_client,
                'function': self._do_restore_backup,
                'kwargs': {'backup': backup,
                           'volume': volume,
                           'context': context}
            }
        except Exception as err:
            with excutils.save_and_reraise_exception():
                msg = (_LE("Restore backup:%(backup_id)s failed, "
                           "err:%(err)s."),
                       {"backup_id": backup_id, "err":err})
                LOG.error(msg)
                self._finish_restore_backup(FAILED, backup, volume)

    def export_record(self, context, backup_id):
        LOG.info(_LI("Export backup record started, backup:%s"), backup_id)
        backup = objects.Backup.get_by_id(context, backup_id)
        if backup.destination == constants.REMOTE_BACKUP:
            availability_zone = backup.replication_zone
        else:
            availability_zone = backup.availability_zone
        backup_record = {
            "backup_type": backup['type'],
            "driver_data": jsonutils.loads(backup['driver_data']),
            "availability_zone": availability_zone
        }
        return backup_record

    def import_record(self, context, backup_id, backup_record):
        LOG.info(_LI('Import record started, backup_record: %s.'),
                 backup_record)
        backup = objects.Backup.get_by_id(context, backup_id)
        backup_type = backup_record.get('backup_record', constants.FULL_BACKUP)
        driver_data = jsonutils.dumps(backup_record.get('driver_data'))

        backup.update({'type': backup_type,
                       'driver_data': driver_data,
                       'status': fields.BackupStatus.AVAILABLE})
        backup.save()

    def _update_snapshot_error(self, snapshot):
        snapshot.update({'status': fields.SnapshotStatus.ERROR})
        snapshot.save()

    def _finish_create_snapshot(self, result, snapshot):
        if result == SUCCEED:
            LOG.info(_LI("create snapshot:%s succeed"), snapshot.id)
            snapshot.update({"status": fields.SnapshotStatus.AVAILABLE})
            snapshot.save()
        else:
            self._update_snapshot_error(snapshot)

    def create_snapshot(self, context, snapshot_id, volume_id):
        LOG.info(_LI("Create snapshot started, snapshot:%(snapshot_id)s, "
                     "volume: %(volume_id)s"),
                 {'volume_id': volume_id, 'snapshot_id': snapshot_id})
        snapshot = objects.Snapshot.get_by_id(context, snapshot_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.SnapshotStatus.CREATING
        actual_status = snapshot['status']
        if actual_status != expected_status:
            msg = (_('Create snapshot aborted, expected snapshot status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_create_snapshot(FAILED, snapshot)
            raise exception.InvalidSnapshot(reason=msg)

        try:
            self.driver.create_snapshot(snapshot=snapshot, volume=volume)
            self.sync_snapshots[snapshot_id] = {
                'action': 'create',
                'snapshot': snapshot
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("create snapshot:%(snapshot_id)s failed, "
                              "err:%(err)s"),
                          {"snapshot_id": snapshot.id, "err": err})
                self._update_snapshot_error(snapshot)

    def _finish_delete_snapshot(self, result, snapshot, status=None):
        if result == SUCCEED:
            LOG.info(_LI("delete snapshot:%s succeed"), snapshot.id)
            snapshot.destroy()
        else:
            status = status if status else fields.SnapshotStatus.ERROR
            snapshot.update({'status': status})
            snapshot.save()

    def delete_snapshot(self, context, snapshot_id):
        LOG.info(_LI("Delete snapshot started, snapshot:%s"), snapshot_id)

        snapshot = objects.Snapshot.get_by_id(context, snapshot_id)

        expected_status = fields.SnapshotStatus.DELETING
        actual_status = snapshot['status']
        if actual_status != expected_status:
            msg = (_('Delete snapshot aborted, expected backup status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_delete_snapshot(FAILED, snapshot)
            raise exception.InvalidBackup(reason=msg)

        try:
            driver_snapshot = self.driver.get_snapshot(snapshot=snapshot)
            LOG.info(_LI("driver snapshot info: %s"), driver_snapshot)
            if driver_snapshot['status'] == fields.SnapshotStatus.DELETED:
                self._finish_delete_snapshot(SUCCEED, snapshot)
                return
            self.driver.delete_snapshot(snapshot=snapshot)
            self.sync_snapshots[snapshot_id] = {
                'action': 'delete',
                'snapshot': snapshot
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("delete snapshot:%(snapshot_id)s failed, "
                              "err:%(err)s"),
                          {"snapshot_id": snapshot_id, "err": err})
                self._finish_delete_snapshot(FAILED, snapshot)

    def _finish_rollback_snapshot(self, result, snapshot, volume, status=None):
        if result == SUCCEED:
            LOG.info(_LI("rollback snapshot:%(snapshot_id)s succeed."),
                     snapshot.id)
            snapshot.destroy()
            volume.update({'status': fields.VolumeStatus.AVAILABLE})
            volume.save()
        else:
            status = status if status else fields.SnapshotStatus.AVAILABLE
            snapshot.update({'status': status})
            snapshot.save()
            volume.update({'status': fields.VolumeStatus.ERROR})
            volume.save()

    def rollback_snapshot(self, context, snapshot_id, volume_id):
        LOG.info(_LI("Rollback snapshot, snapshot_id %s"), snapshot_id)

        snapshot = objects.Snapshot.get_by_id(context, snapshot_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.VolumeStatus.ROLLING_BACK
        actual_status = volume['status']
        if actual_status != expected_status:
            msg = (_('Rollback snapshot aborted, expected volume status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_rollback_snapshot(FAILED, snapshot, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            self.driver.rollback_snapshot(snapshot)
            self.sync_snapshots[snapshot_id] = {
                'snapshot': snapshot,
                'volume': volume,
                'action': 'rollback'
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("rollback snapshot:%(snapshot_id)s failed, "
                              "err:%(err)s"),
                          {"snapshot_id": snapshot_id, "err": err})
                self._finish_rollback_snapshot(FAILED, snapshot, volume)

    def _finish_enable_replicate(self, result, volume, status=None):
        if result == SUCCEED:
            volume.update({'replicate_status': fields.ReplicateStatus.ENABLED})
            volume.save()
        else:
            if status is None:
                status = fields.ReplicateStatus.ERROR
            volume.update({'replicate_status': status})
            volume.save()

    def create_replicate(self, context, volume_id):
        LOG.info(_LI("Create replicate started, volume:%s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.ReplicateStatus.ENABLING
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Create replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_enable_replicate(FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            self.driver.create_replicate(volume=volume)
            self.sync_replicates[volume_id] = {
                'action': 'create_replicate',
                'volume': volume
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("create volume:%(volume_id)s replicate failed, "
                              "err:%(err)s"),
                          {"volume_id": volume_id, "err": err})
                self._finish_enable_replicate(FAILED, volume)

    def enable_replicate(self, context, volume_id):
        LOG.info(_LI("Enable replicate started, volume:%s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.ReplicateStatus.ENABLING
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Enable replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_enable_replicate(FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            self.driver.enable_replicate(volume=volume)
            self.sync_replicates[volume_id] = {
                'action': 'enable_replicate',
                'volume': volume
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("enable volume:%(volume_id)s replicate failed, "
                              "err:%(err)s"),
                          {"volume_id": volume_id, "err": err})
                self._finish_enable_replicate(FAILED, volume)

    def _finish_disable_replicate(self, result, volume, status=None):
        if result == SUCCEED:
            volume.update(
                {'replicate_status': fields.ReplicateStatus.DISABLED})
            volume.save()
        else:
            if status is None:
                status = fields.ReplicateStatus.ERROR
            volume.update({'replicate_status': status})
            volume.save()

    def disable_replicate(self, context, volume_id):
        LOG.info(_LI("Disable replicate started, volume:%s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.ReplicateStatus.DISABLING
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Disable replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_disable_replicate(FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            self.driver.disable_replicate(volume=volume)
            self.sync_replicates[volume_id] = {
                'action': 'disable_replicate',
                'volume': volume
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("disable volume:%(volume_id)s replicate failed, "
                              "err:%(err)s"),
                          {"volume_id": volume_id, "err": err})
                self._finish_disable_replicate(FAILED, volume)

    def _finish_delete_replicate(self, result, volume, status=None):
        if result == SUCCEED:
            volume.update({'replicate_status': fields.ReplicateStatus.DELETED,
                           'replicate_mode': None,
                           'replication_id': None,
                           'peer_volume': None,
                           'access_mode': None})
            volume.save()
        else:
            if status is None:
                status = fields.ReplicateStatus.ERROR
            volume.update({'replicate_status': status})
            volume.save()

    def delete_replicate(self, context, volume_id):
        LOG.info(_LI("Delete replicate started, volume:%s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.ReplicateStatus.DELETING
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Delete replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_delete_replicate(FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            self.driver.delete_replicate(volume=volume)
            self.sync_replicates[volume_id] = {
                'action': 'delete_replicate',
                'volume': volume
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("delete volume:%(volume_id)s replicate failed, "
                              "err:%(err)s"),
                          {"volume_id": volume_id, "err": err})
                self._finish_delete_replicate(FAILED, volume)

    def _finish_failover_replicate(self, result, volume, status=None):
        if result == SUCCEED:
            volume.update(
                {'replicate_status': fields.ReplicateStatus.FAILED_OVER})
            volume.save()
        else:
            if status is None:
                status = fields.ReplicateStatus.ERROR
            volume.update({'replicate_status': status})
            volume.save()

    def failover_replicate(self, context, volume_id, checkpoint_id=None,
                           snapshot_id=None, force=False):
        LOG.info(_LI("Failover replicate started, volume:%s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.ReplicateStatus.FAILING_OVER
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Failover replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_failover_replicate(FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            self.driver.failover_replicate(volume=volume,
                                           checkpoint_id=checkpoint_id,
                                           snapshot_id=snapshot_id)
            self.sync_replicates[volume_id] = {
                'action': 'failover_replicate',
                'volume': volume
            }
            if snapshot_id:
                snapshot = objects.Snapshot.get_by_id(context, snapshot_id)
                self.sync_snapshots[snapshot_id] = {
                    'action': 'create',
                    'snapshot': snapshot
                }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("failover volume:%(volume_id)s replicate "
                              "failed, err:%(err)s"),
                          {"volume_id": volume_id, "err": err})
                self._finish_failover_replicate(FAILED, volume)

    def _finish_reverse_replicate(self, result, volume, driver_volume=None):
        if result == SUCCEED:
            volume.update(
                {'replicate_status': fields.ReplicateStatus.DISABLED})
            replicate_mode = driver_volume['replicate_mode']
            if replicate_mode == constants.REP_MASTER:
                volume.update({'replicate_mode': constants.REP_MASTER,
                               'access_mode': constants.ACCESS_RW})
            else:
                volume.update({'replicate_mode': constants.REP_SLAVE,
                               'access_mode': constants.ACCESS_RO})
            volume.save()
        else:
            if driver_volume is None:
                status = fields.ReplicateStatus.ERROR
            else:
                status = driver_volume['replicate_status']
            volume.update({'replicate_status': status})
            volume.save()

    def reverse_replicate(self, context, volume_id):
        LOG.info(_LI("Reverse replicate started, volume:%s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.ReplicateStatus.REVERSING
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Reverse replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_reverse_replicate(FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            self.driver.reverse_replicate(volume=volume)
            self.sync_replicates[volume_id] = {
                'action': 'reverse_replicate',
                'volume': volume
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("reverse volume:%(volume_id)s replicate "
                              "failed, err:%(err)s"),
                          {"volume_id": volume_id, "err": err})
                self._finish_reverse_replicate(FAILED, volume)

    def _do_create_volume(self, sync_result, context, mountpoint, snapshot,
                          volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        if sync_result == 'succeed':
            try:
                device = self._get_attach_device(mountpoint)
                driver_volume = self.driver.query_volume_from_snapshot(
                    volume.id)
                if driver_volume['status'] == fields.VolumeStatus.DELETED:
                    self.driver.create_volume_from_snapshot(snapshot,
                                                            volume.id,
                                                            device)
                retry_attempts = CONF.retry_attempts
                while retry_attempts != 0:
                    try:
                        driver_volume = self.driver.query_volume_from_snapshot(
                            volume.id)
                        LOG.info("Driver volume info: %s" % driver_volume)
                        if (driver_volume['status']
                                == fields.VolumeStatus.ENABLED):
                            volume.update(
                                {'status': fields.VolumeStatus.AVAILABLE})
                            volume.save()
                            break
                    except Exception:
                        pass
                    retry_attempts -= 1
                    greenthread.sleep(CONF.sync_status_interval)
                if retry_attempts == 0:
                    self._finish_create_volume(FAILED, volume, snapshot)
                    return
                self._detach_volume_from_sg(context, logical_volume_id)
                cinder_client = self._create_cinder_client(context)
                self.sync_detach_volumes[logical_volume_id] = {
                    'cinder_client': cinder_client,
                    'function': self._finish_create_volume,
                    'kwargs': {'volume': volume}
                }
            except Exception:
                self._finish_create_volume(FAILED, volume, snapshot)
        else:
            self._finish_create_volume(FAILED, volume, snapshot)

    def _finish_create_volume(self, result, volume, snapshot):
        if result == SUCCEED:
            LOG.info(_LI('create volume:%(volume_id)s from '
                         'snapshot:%(snapshot_id)s succeed'),
                     {"snapshot_id": snapshot.id, "volume_id": volume.id})
            volume.update({'status': fields.VolumeStatus.AVAILABLE})
            volume.save()
        else:
            LOG.info(_LI('create volume:%(volume_id)s from '
                         'snapshot:%(snapshot_id)s failed'),
                     {"snapshot_id": snapshot.id, "volume_id": volume.id})
            volume.update({'status': fields.VolumeStatus.ERROR})
            volume.save()

    def create_volume(self, context, snapshot_id, volume_id):
        LOG.info(_LI("Create new volume from snapshot, snapshot_id %s"),
                 snapshot_id)
        snapshot = objects.Snapshot.get_by_id(context, snapshot_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume_id
        cinder_client = cinder.get_project_context_client(context)
        try:
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
        except Exception as err:
            msg = (_("Get the volume '%s' from cinder failed."),
                   logical_volume_id)
            LOG.error(msg)
            volume.update({'status': fields.VolumeStatus.ERROR})
            volume.save()
            raise exception.CinderClientError(reason=msg)

        retry = CONF.retry_attempts
        while cinder_volume.status == 'creating' and retry != 0:
            greenthread.sleep(CONF.sync_status_interval)
            try:
                cinder_volume = cinder_client.volumes.get(logical_volume_id)
            except Exception:
                msg = (_("Get the volume '%s' from cinder failed."),
                       logical_volume_id)
                LOG.error(msg)
                volume.update({'status': fields.VolumeStatus.ERROR})
                volume.save()
                raise exception.InvalidVolume(reason=msg)
            retry -= 1
        if retry == 0:
            msg = (_("Wait cinder volume %s to be available timeout."),
                   logical_volume_id)
            LOG.error(msg)
            volume.update({'status': fields.VolumeStatus.ERROR})
            volume.save()
            raise exception.InvalidVolume(reason=msg)
        if cinder_volume.status != 'available':
            msg = (_("Wait new cinder volume %s to be available failed."),
                   logical_volume_id)
            LOG.error(msg)
            volume.update({'status': fields.VolumeStatus.ERROR})
            volume.save()
            raise exception.InvalidVolume(reason=msg)

        try:
            self._attach_volume_to_sg(context, logical_volume_id)
            self.sync_attach_volumes[logical_volume_id] = {
                'cinder_client': cinder_client,
                'function': self._do_create_volume,
                'kwargs': {'snapshot': snapshot,
                           'volume': volume,
                           'context': context}}
        except Exception as err:
            msg = (_("Attach volume to sg failed, err: %s."), err)
            LOG.error(msg)
            volume.update({'status': fields.VolumeStatus.ERROR})
            volume.save()
            raise exception.AttachSGFailed(reason=msg)
