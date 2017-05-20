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
from retrying import retry

from cinderclient import exceptions as cinder_exc
from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_serialization import jsonutils
from oslo_service import loopingcall
from oslo_utils import excutils
from oslo_utils import importutils

from wormholeclient.client import Client as AgentClient
from sgservice.common import constants
from sgservice.common.clients import cinder
from sgservice.common.clients import nova
from sgservice import context as sg_context
from sgservice import exception
from sgservice.i18n import _, _LE, _LI
from sgservice import manager
from sgservice import objects
from sgservice.objects import fields

controller_manager_opts = [
    cfg.IntOpt('sync_status_interval',
               default=60,
               help='sync resources status interval'),
    cfg.StrOpt('sg_driver',
               default='sgservice.controller.drivers.iscsi.ISCSISGDriver',
               help='The class name of storage gateway driver'),
    cfg.IntOpt('retry_attempts',
               default=10),
    cfg.PortOpt('sgs_agent_port',
                default=8989,
                help='The port of sgs agent')
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
    cfg.StrOpt('sg_client_mode',
               default='iscsi',
               help='The mode of sg_client')
]

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

CONF.register_opts(controller_manager_opts)
CONF.register_opts(sg_client_opts, group='sg_client')

SYNC_SUCCEED = 'succeed'
SYNC_FAILED = 'failed'

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


class SGClientObject(object):
    def __init__(self,
                 sg_client_info=None,
                 instance=CONF.sg_client.sg_client_instance,
                 host=CONF.sg_client.sg_client_host):
        self.instance = instance
        self.host = host
        self.port = CONF.sg_client.sg_client_port
        if sg_client_info:
            try:
                sg_client_dict = jsonutils.loads(sg_client_info)
                self.instance = sg_client_dict.get('instance')
                self.host = sg_client_dict.get('host')
            except Exception:
                pass

    def dumps(self):
        return jsonutils.dumps({'host': self.host, 'instance': self.instance})


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
        self.system_sg_client = SGClientObject()

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

        self.sync_vols_from_snap = {}
        sync_vols_from_snap_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_vols_from_snap)
        sync_vols_from_snap_loop.start(
            interval=self.sync_status_interval,
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
            elif volume.status == fields.VolumeStatus.ATTACHING:
                self._init_attaching_volume(context, volume)
            elif volume.status == fields.VolumeStatus.DETACHING:
                self._init_detaching_volume(context, volume)

    def _init_enabling_volume(self, context, volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            sg_client = SGClientObject()
            if volume.sg_client is None:
                volume.update({'sg_client': sg_client.dumps()})
                volume.save()
            if cinder_volume.status in ['available', 'attaching']:
                if cinder_volume.status == 'available':
                    self._attach_volume_to_sg(context, logical_volume_id,
                                              sg_client)
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'in-use')
            if (cinder_volume.status == 'in-use'
                and len(cinder_volume.attachments) == 1
                and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                mountpoint = cinder_volume.attachments[0]['device']
                device = self._get_attach_device(sg_client, mountpoint)
                self.driver.enable_sg(sg_client, volume, device)
                self._finish_enable_sg(SYNC_SUCCEED, volume)
            else:
                self._finish_enable_sg(SYNC_FAILED, volume)
        except Exception:
            self._finish_enable_sg(SYNC_FAILED, volume)

    def _init_deleting_volume(self, context, volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            sg_client = SGClientObject(volume.sg_client)
            if (cinder_volume.status in ['in-use', 'detaching']
                and len(cinder_volume.attachments) == 1
                and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                if cinder_volume.status == 'in-use':
                    driver_volume = self.driver.get(volume)
                    LOG.info(_LI('driver_volume info:%s'), driver_volume)
                    if driver_volume['status'] != fields.VolumeStatus.DELETED:
                        self.driver.disable_sg(sg_client, volume)
                    self._detach_volume_from_sg(context, logical_volume_id,
                                                sg_client)
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'available')
                if cinder_volume.status == 'available':
                    self._finish_delete_volume(SYNC_SUCCEED, volume)
                else:
                    self._finish_delete_volume(SYNC_FAILED, volume)
            elif cinder_volume.status == 'available':
                self._finish_delete_volume(SYNC_SUCCEED, volume)
            else:
                self._finish_delete_volume(SYNC_FAILED, volume)
        except cinder_exc.NotFound:
            self._finish_delete_volume(SYNC_SUCCEED, volume)
        except Exception:
            self._finish_delete_volume(SYNC_FAILED, volume)

    def _init_creating_volume(self, context, volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        snapshot = objects.Snapshot.get_by_id(context, volume.snapshot_id)
        if snapshot.sg_client != volume.sg_client:
            volume.update({'sg_client': snapshot.sg_client})
            volume.save()
        try:
            sg_client = SGClientObject(volume.sg_client)
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            # cinder volume still creating
            if cinder_volume.status in ['creating', 'available']:
                if cinder_volume.status == 'available':
                    driver_volume = self.driver.query_volume_from_snapshot(
                        sg_client, volume)
                    if driver_volume['status'] == fields.VolumeStatus.ENABLED:
                        self._finish_create_volume(SYNC_SUCCEED, volume,
                                                   snapshot)
                        return
                self._do_create_volume(context, snapshot, volume)
            # cinder volume is in-use or attaching
            # cinder volume is in-use or attaching
            elif cinder_volume.status in ['attaching', 'in-use']:
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'in-use')
                # cinder volume already attach to sg_client
                if (cinder_volume.status == 'in-use'
                    and len(cinder_volume.attachments) == 1
                    and cinder_volume.attachments[0]['server_id']
                        == sg_client.instance):
                    mountpoint = cinder_volume.attachments[0]['device']
                    # copy snapshot to device
                    device = self._get_attach_device(sg_client, mountpoint)
                    driver_volume = self.driver.query_volume_from_snapshot(
                        sg_client, volume.id)
                    if driver_volume['status'] == fields.VolumeStatus.DELETED:
                        self.driver.create_volume_from_snapshot(
                            sg_client, snapshot, volume.id, device)
                    # wait vol available
                    self._wait_vols_from_snap(volume)
                    self._finish_create_volume(SYNC_SUCCEED, volume, snapshot)
                else:
                    self._finish_create_volume(SYNC_FAILED, volume, snapshot)
            # cinder volume detach from sg_client
            elif (cinder_volume.status == 'detaching'
                  and len(cinder_volume.attachments) == 1
                  and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'available')
                if cinder_volume.status == 'available':
                    self._finish_create_volume(SYNC_SUCCEED, volume, snapshot)
                else:
                    self._finish_create_volume(SYNC_FAILED, volume, snapshot)
            else:
                self._finish_create_volume(SYNC_FAILED, volume, snapshot)
        except Exception:
            self._finish_create_volume(SYNC_FAILED, volume, snapshot)

    def _init_restoring_volume(self, context, volume, backup):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        if backup.sg_client != volume.sg_client:
            volume.update({'sg_client': backup.sg_client})
            volume.save()
        try:
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            sg_client = SGClientObject(volume.sg_client)
            # cinder volume still not attach to sg_client
            if cinder_volume.status == 'available':
                self._do_restore_backup(context, backup, volume)
            # cinder volume already attached to sg_client
            elif cinder_volume.status == 'attaching':
                self._attach_volume_to_sg(context, logical_volume_id,
                                          sg_client)
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'in-use')
            if (cinder_volume.status == 'in-use'
                and len(cinder_volume.attachments) == 1
                and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                mountpoint = cinder_volume.attachments[0]['device']
                device = self._get_attach_device(sg_client, mountpoint)
                # TODO(luobin): current restore is a synchronization function
                self.driver.restore_backup(sg_client, backup, volume, device)
                self._detach_volume_from_sg(context, logical_volume_id,
                                            sg_client)
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'available')
            if (cinder_volume.status == 'detaching'
                  and len(cinder_volume.attachments) == 1
                  and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'available')
                if cinder_volume.status == 'available':
                    self._finish_restore_backup(SYNC_SUCCEED, backup, volume)
                else:
                    self._finish_restore_backup(SYNC_FAILED, backup, volume)
            else:
                self._finish_restore_backup(SYNC_FAILED, backup, volume)
        except Exception:
            self._finish_restore_backup(SYNC_FAILED, backup, volume)

    def _init_attaching_volume(self, context, volume):
        attachment = objects.VolumeAttachmentList.get_all_by_volume_id(
            context, volume.id)[-1]
        if CONF.sg_client.sg_client_mode == 'iscsi':
            self._do_iscsi_attach_volume(volume, attachment)
        else:
            self._redo_agent_attach_volume(context, volume, attachment)

    def _init_detaching_volume(self, context, volume):
        attachment = objects.VolumeAttachmentList.get_all_by_volume_id(
            context, volume.id)[-1]
        if CONF.sg_client.sg_client_mode == 'iscsi':
            self._do_iscsi_detach_volume(volume, attachment)
        else:
            self._redo_agent_detach_volume(context, volume, attachment)

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

    def _attach_volume_to_sg(self, context, volume_id, sg_client):
        """ Attach volume to sg-client and get mountpoint
        """
        try:
            nova_client = self._create_nova_client(context)
            volume_attachment = nova_client.volumes.create_server_volume(
                sg_client.instance, volume_id)
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
                    self._finish_delete_backup(SYNC_SUCCEED, backup)
                else:
                    self._finish_delete_backup(SYNC_FAILED, backup,
                                               driver_backup['status'])
            elif action == 'create':
                volume = backup_info['volume']
                if driver_backup['status'] == fields.BackupStatus.AVAILABLE:
                    self._finish_create_backup(SYNC_SUCCEED, volume, backup)
                else:
                    self._finish_create_backup(SYNC_FAILED, volume, backup)
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
                    self._finish_delete_snapshot(SYNC_SUCCEED, snapshot)
                else:
                    self._finish_delete_snapshot(SYNC_FAILED, snapshot,
                                                 driver_snapshot['status'])
            elif action == 'create':
                if (driver_snapshot['status']
                        == fields.SnapshotStatus.AVAILABLE):
                    self._finish_create_snapshot(SYNC_SUCCEED, snapshot)
                else:
                    self._finish_create_snapshot(SYNC_FAILED, snapshot)
            elif action == 'rollback':
                volume = snapshot_info['volume']
                if driver_snapshot['status'] == fields.SnapshotStatus.DELETED:
                    self._finish_rollback_snapshot(SYNC_SUCCEED, snapshot,
                                                   volume)
                else:
                    self._finish_rollback_snapshot(SYNC_FAILED, snapshot,
                                                   volume,
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
                    self._finish_enable_replicate(SYNC_SUCCEED, volume)
                else:
                    self._finish_enable_replicate(SYNC_FAILED, volume,
                                                  replicate_status)
            elif action == 'delete_replicate':
                if replicate_status in [None, fields.ReplicateStatus.DELETED]:
                    self._finish_delete_replicate(SYNC_SUCCEED, volume)
                else:
                    self._finish_delete_replicate(SYNC_FAILED, volume,
                                                  replicate_status)
            elif action == 'failover_replicate':
                if replicate_status == fields.ReplicateStatus.FAILED_OVER:
                    self._finish_failover_replicate(SYNC_SUCCEED, volume)
                else:
                    self._finish_failover_replicate(SYNC_FAILED, volume,
                                                    replicate_status)
            elif action == 'disable_replicate':
                if replicate_status == fields.ReplicateStatus.DISABLED:
                    self._finish_disable_replicate(SYNC_SUCCEED, volume)
                else:
                    self._finish_disable_replicate(SYNC_FAILED, volume,
                                                   replicate_status)
            elif action == 'reverse_replicate':
                if replicate_status == fields.ReplicateStatus.DISABLED:
                    self._finish_reverse_replicate(SYNC_SUCCEED, volume,
                                                   driver_volume)
                else:
                    self._finish_reverse_replicate(SYNC_FAILED, volume,
                                                   driver_volume)
            self.sync_replicates.pop(volume_id)

    def _get_attach_device(self, sg_client, mountpoint):
        try:
            devices = self.driver.list_devices(sg_client)
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

    def _detach_volume_from_sg(self, context, volume_id, sg_client):
        # detach volume from sg-client
        try:
            nova_client = self._create_nova_client(context)
            nova_client.volumes.delete_server_volume(sg_client.instance,
                                                     volume_id)
        except Exception as err:
            LOG.error(err)
            raise exception.DetachSGFailed(reason=err)

    def _sync_vols_from_snap(self):
        for volume_id, volume_info in self.sync_vols_from_snap.items():
            volume = volume_info['volume']
            snapshot = volume_info['snapshot']
            context = volume_info['context']
            sg_client = SGClientObject(volume.sg_client)
            try:
                driver_volume = self.driver.query_volume_from_snapshot(
                    sg_client, volume_id)
                if driver_volume['status'] == fields.VolumeStatus.ENABLED:
                    if 'logicalVolumeId' in volume.metadata:
                        logical_volume_id = volume.metadata['logicalVolumeId']
                    else:
                        logical_volume_id = volume_id
                    self._detach_volume_from_sg(context, logical_volume_id,
                                                sg_client)
                    cinder_client = self._create_cinder_client(context)
                    self.sync_detaching_volumes[logical_volume_id] = {
                        'cinder_client': cinder_client,
                        'function': self._finish_create_volume,
                        'kwargs': {'volume': volume}
                    }
                    self.sync_vols_from_snap.pop(volume_id)
            except Exception:
                pass

    def delete(self, context, volume_id):
        LOG.info(_LI("Delete sg volume with id %s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)
        self._do_delete_volume(context, volume)

    def _finish_enable_sg(self, sync_result, volume):
        if sync_result == SYNC_SUCCEED:
            LOG.info(_LI('enable sg succeed, volume_id:%s'), volume['id'])
            volume.update({'status': fields.VolumeStatus.ENABLED})
            volume.save()
        else:
            LOG.error(_LE('enable sg failed, volume_id: %s'), volume['id'])
            volume.update({'status': fields.VolumeStatus.ERROR})
            volume.save()

    def _do_enable_sg(self, context, volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            # attach cinder volume to sg_client
            sg_client = SGClientObject(volume.sg_client)
            cinder_client = self._create_cinder_client(context)
            self._attach_volume_to_sg(context, logical_volume_id, sg_client)
            cinder_volume = self._wait_cinder_volume_status(
                cinder_client, logical_volume_id, 'in-use')
            if cinder_volume.status != 'in-use':
                self._finish_enable_sg(SYNC_FAILED, volume)
                return
            mountpoint = cinder_volume.attachments[0]['device']
            # enable sg in sg_client
            device = self._get_attach_device(sg_client, mountpoint)
            self.driver.enable_sg(sg_client, volume, device)
            self._finish_enable_sg(SYNC_SUCCEED, volume)
        except Exception:
            self._finish_enable_sg(SYNC_FAILED, volume)

    def enable_sg(self, context, volume_id):
        LOG.info(_LI("Enable-SG for this volume with id %s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)
        sg_client = SGClientObject()
        volume.update({'replication_zone': CONF.sg_client.replication_zone,
                       'sg_client': sg_client.dumps()})
        volume.save()
        self._do_enable_sg(context, volume)

    def _finish_delete_volume(self, sync_result, volume):
        if sync_result == SYNC_SUCCEED:
            LOG.info(_LI('Delete or disable sg volume succeed, volume_id:%s'),
                     volume['id'])
            volume.destroy()
        else:
            LOG.error(_LE('Delete or disable sg volume failed, volume_id: %s'),
                      volume['id'])
            volume.update({'status': fields.VolumeStatus.ERROR})
            volume.save()

    def _do_delete_volume(self, context, volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            if volume.sg_client is not None:
                sg_client = SGClientObject(volume.sg_client)
                cinder_client = self._create_cinder_client(context)
                driver_volume = self.driver.get(sg_client, volume)
                if driver_volume['status'] != fields.VolumeStatus.DELETED:
                    self.driver.disable_sg(sg_client, volume)
                self._detach_volume_from_sg(context, logical_volume_id,
                                            sg_client)
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'available')
                if cinder_volume.status != 'available':
                    self._finish_delete_volume(SYNC_FAILED, volume)
                else:
                    self._finish_delete_volume(SYNC_SUCCEED, volume)
            else:
                self._finish_delete_volume(SYNC_SUCCEED, volume)
        except Exception:
            self._finish_delete_volume(SYNC_FAILED, volume)

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
        self._do_delete_volume(context, volume)

    def _do_agent_attach_volume(self, context, volume, attachment):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        # detach from system sg_client
        try:
            cinder_client = self._create_cinder_client(context)
            sg_client = SGClientObject(volume.sg_client)
            # detach from system sg_client
            self._detach_volume_from_sg(context, logical_volume_id,
                                        sg_client)
            cinder_volume = self._wait_cinder_volume_status(
                cinder_client, logical_volume_id, 'available')
            if cinder_volume.status != 'available':
                self._rollback_agent_attach_volume(context, volume, attachment)
                return
        except Exception as err:
            LOG.error(_LE("attach agent mode volume failed, err:%s"), err)
            self._rollback_agent_attach_volume(context, volume, attachment)
            return
        # attach to guest sg_client
        try:
            instance_id = attachment.logical_instance_id
            instance_host = attachment.instance_host
            sg_client = SGClientObject(instance=instance_id,
                                       host=instance_host)
            volume.update({'sg_client': sg_client.dumps()})
            volume.save()
            self._attach_volume_to_sg(context, logical_volume_id, sg_client)
            cinder_volume = self._wait_cinder_volume_status(
                cinder_client, logical_volume_id, 'in-use')
            if cinder_volume.status != 'in-use':
                self._rollback_agent_attach_volume(context, volume, attachment)
        except Exception as err:
            LOG.error(_LE("attach agent mode volume failed, err:%s"), err)
            self._rollback_agent_attach_volume(context, volume, attachment)
            return
        # attach volume in guest sg_client
        try:
            mountpoint = cinder_volume.attachments[0]['device']
            device = self._get_attach_device(sg_client, mountpoint)
            self.driver.attach_volume(sg_client, volume, device)
            driver_data = {'driver_type': 'agent'}
            volume.update({'driver_data': jsonutils.dumps(driver_data)})
            self._finish_attach_volume(SYNC_SUCCEED, volume, attachment,
                                       device)
        except Exception as err:
            LOG.error(_LE("attach agent mode volume failed, err:%s"), err)
            self._rollback_agent_attach_volume(context, volume, attachment)
            return

    def _do_iscsi_attach_volume(self, volume, attachment):
        # initialize connection
        try:
            sg_client = SGClientObject(volume.sg_client)
            connection_info = self.driver.initialize_connection(sg_client,
                                                                volume)
        except Exception as err:
            LOG.error(_LE("attach iscsi mode volume failed, err:%s"), err)
            self._finish_attach_volume(SYNC_FAILED, volume, attachment)
            return
        # use iscsi agent attach volume
        try:
            instance_host = attachment.instance_host
            sgs_agent = AgentClient(instance_host, CONF.sgs_aggent_port)
            mountpoint = sgs_agent.connect_volume(connection_info)['mountpoint']
        except Exception as err:
            LOG.error(_LE("attach iscsi mode volume failed, err:%s"), err)
            # terminate connection rollback attach
            self.driver.terminate_connection(sg_client, volume)
            self._finish_attach_volume(SYNC_FAILED, volume, attachment)
            return

        driver_data = {'driver_type': 'iscsi',
                       'connection_info': connection_info}
        volume.update({'driver_data': jsonutils.dumps(driver_data)})
        volume.save()
        self._finish_attach_volume(SYNC_SUCCEED, volume, attachment,
                                   mountpoint=mountpoint)

    def _rollback_agent_attach_volume(self, context, volume, attachment):
        self._redo_agent_detach_volume(context, volume, attachment)

    # used to redo detach-volume or rollback attach-volume
    def _redo_agent_detach_volume(self, context, volume, attachment):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            sg_client = SGClientObject(volume.sg_client)
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            if sg_client.instance == attachment.logical_instance_id:
                if cinder_volume.status == 'in-use':
                    self.driver.detach_volume(sg_client, volume)
                    self._detach_volume_from_sg(context, logical_volume_id,
                                                sg_client)
                    cinder_volume = cinder_client.volumes.get(
                        logical_volume_id)
                if cinder_volume.status == 'detaching':
                    cinder_volume = self._wait_cinder_volume_status(
                        cinder_client, logical_volume_id, 'available')
                if cinder_volume.status != 'available':
                    self._finish_detach_volume(SYNC_FAILED, volume, attachment)
                    return
                sg_client = SGClientObject()
                volume.update({'sg_client': sg_client.dumps()})
                volume.save()
            if cinder_volume.status == 'available':
                self._attach_volume_to_sg(context, logical_volume_id,
                                          sg_client)
                cinder_volume = cinder_client.volumes.get(logical_volume_id)
            if cinder_volume.status == 'attaching':
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'in-use')
            if cinder_volume.status != 'in-use':
                self._finish_detach_volume(SYNC_FAILED, volume, attachment)
            else:
                self._finish_detach_volume(SYNC_SUCCEED, volume, attachment)
        except Exception as err:
            LOG.info(_LI("redo detach agent volume faile, err:%s"), err)
            self._finish_detach_volume(SYNC_FAILED, volume, attachment)

    def _finish_attach_volume(self, sync_result, volume, attachment,
                              mountpoint=None,
                              status=fields.VolumeStatus.ERROR):
        if sync_result == SYNC_SUCCEED:
            attachment.finish_attach(mountpoint)
        else:
            attachment.destroy()
            status = status if status else volume.previous_status
            volume.status = status
            volume.save()

    def attach_volume(self, context, volume_id, attachment_id):
        attachment = objects.VolumeAttachment.get_by_id(context, attachment_id)
        instance_uuid = attachment.instance_uuid
        LOG.info(_LI("Attach volume '%s' to '%s'"), volume_id, instance_uuid)
        volume = objects.Volume.get_by_id(context, volume_id)

        try:
            if CONF.sg_client.sg_client_mode == 'iscsi':
                self._do_iscsi_attach_volume(volume, attachment)
            else:
                self._do_agent_attach_volume(context, volume, attachment)
        except Exception as err:
            msg = _LE("Attach volume failed, err:%s" % err)
            LOG.error(msg)
            self._finish_attach_volume(SYNC_FAILED, volume, attachment)

    def _do_iscsi_detach_volume(self, volume, attachment):
        driver_data = jsonutils.loads(volume.driver_data)
        connection_info = driver_data['connection_info']
        instance_host = attachment.instance_host
        try:
            sgs_agent = AgentClient(instance_host, CONF.sgs_aggent_port)
            sgs_agent.disconnect_volume(connection_info)
            self._finish_detach_volume(SYNC_SUCCEED, volume, attachment)
        except Exception:
            self._finish_detach_volume(SYNC_FAILED, volume, attachment)

    def _do_agent_detach_volume(self, context, volume, attachment):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            # detach volume in guest vm sg-client
            sg_client = SGClientObject(volume.sg_client)
            self.driver.detach_volume(sg_client, volume)
            # detach volume from guest vm
            self._detach_volume_from_sg(context, logical_volume_id,
                                        sg_client)
            cinder_client = self._create_cinder_client(context)
            cinder_volume = self._wait_cinder_volume_status(cinder_client,
                                                            logical_volume_id,
                                                            'available')
            if cinder_volume.status != 'available':
                self._rollback_agent_detach_volume(context, volume, attachment)
                return
            # attach volume to system sg-client
            sg_client = SGClientObject()
            volume.update({'sg_client': sg_client.dumps()})
            volume.save()
            self._attach_volume_to_sg(context, logical_volume_id,
                                      sg_client)
            cinder_volume = self._wait_cinder_volume_status(
                cinder_client, logical_volume_id, 'in-use')
            if cinder_volume.status != 'in-use':
                self._rollback_agent_detach_volume(context, volume, attachment)
            mountpoint = cinder_volume.attachments[0]['device']
            device = self._get_attach_device(sg_client, mountpoint)
            self.driver.enable_sg(sg_client, volume, device)
            self._finish_detach_volume(SYNC_SUCCEED, volume, attachment)
        except Exception as err:
            self._rollback_agent_detach_volume(context, volume, attachment)

    def _rollback_agent_detach_volume(self, context, volume, attachment):
        self._redo_agent_attach_volume(context, volume, attachment)

    # used to redo attach volume or rollback detach volume
    def _redo_agent_attach_volume(self, context, volume, attachment):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        sg_client = SGClientObject(volume.sg_client)
        try:
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            if sg_client.instance == CONF.sg_client.sg_client_instance:
                if cinder_volume.status == 'in-use':
                    self._detach_volume_from_sg(context,
                                                logical_volume_id, sg_client)
                    cinder_volume = cinder_client.volumes.get(
                        logical_volume_id)
                if cinder_volume.status == 'detaching':
                    cinder_volume = self._wait_cinder_volume_status(
                        cinder_client, logical_volume_id, 'available')
                if cinder_volume.status != 'available':
                    LOG.info(_LI("use nova to detach volume from system "
                                 "sg-client failed"))
                    self._finish_attach_volume(SYNC_FAILED, volume, attachment)
                instance_id = attachment.logical_instance_id
                instance_host = attachment.instance_host
                sg_client = SGClientObject(instance=instance_id,
                                           host=instance_host)
                volume.update({'sg_client': sg_client.dumps()})
                volume.save()
            if cinder_volume.status == 'available':
                self._attach_volume_to_sg(context, logical_volume_id,
                                          sg_client)
                cinder_volume = cinder_client.volumes.get(logical_volume_id)
            if cinder_volume.status == 'attaching':
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'in-use')
            if cinder_volume.status != 'in-use':
                LOG.info(_LI("use nova to attach volume to guest vm failed"))
                self._finish_attach_volume(SYNC_FAILED, volume, attachment)
            else:
                mountpoint = cinder_volume.attachments[0]['device']
                device = self._get_attach_device(sg_client, mountpoint)
                self.driver.attach_volume(sg_client, volume, device)
                driver_data = {'driver_type': 'agent'}
                volume.update({'driver_data': jsonutils.dumps(driver_data)})
                self._finish_attach_volume(SYNC_SUCCEED, volume, attachment,
                                           device)
        except Exception as err:
            LOG.error(_LE("redo agent attach volume failed, err:%s"), err)
            self._finish_attach_volume(SYNC_FAILED, volume, attachment)

    def _finish_detach_volume(self, sync_result, volume, attachment,
                              status=fields.VolumeStatus.ERROR):
        if sync_result == SYNC_SUCCEED:
            volume.finish_detach(attachment.id)
        else:
            attachment.destroy()
            volume.update({'status': status})
            volume.save()

    def detach_volume(self, context, volume_id, attachment_id):
        attachment = objects.VolumeAttachment.get_by_id(context, attachment_id)
        instance_uuid = attachment.instance_uuid
        LOG.info(_LI("Detach volume '%s' from '%s'"), volume_id, instance_uuid)
        volume = objects.Volume.get_by_id(context, volume_id)

        try:
            driver_data = jsonutils.loads(volume.driver_data)
            driver_type = driver_data['driver_type']
            if driver_type == 'iscsi':
                self._do_iscsi_detach_volume(volume, attachment)
            else:
                self._do_agent_detach_volume(context, volume, attachment)
        except Exception as err:
            msg = _LE("Attach volume failed, err:%s" % err)
            LOG.error(msg)
            self._finish_detach_volume(SYNC_FAILED, volume, attachment)

    def _update_backup_error(self, backup):
        backup.update({'status': fields.BackupStatus.ERROR})
        backup.save()

    def _finish_create_backup(self, result, volume, backup):
        if result == SYNC_SUCCEED:
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
        backup.update({'sg_client': volume.sg_client})
        backup.save()

        expected_status = 'backing-up'
        actual_status = volume['status']
        if actual_status != expected_status:
            msg = (_('Create backup aborted, expected volume status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._finish_create_backup(SYNC_FAILED, volume, backup)
            raise exception.InvalidVolume(reason=msg)

        expected_status = fields.BackupStatus.CREATING
        actual_status = backup['status']
        if actual_status != expected_status:
            msg = (_('Create backup aborted, expected backup status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._finish_create_backup(SYNC_FAILED, volume, backup)
            raise exception.InvalidBackup(reason=msg)

        try:
            sg_client = SGClientObject(backup.sg_client)
            self.driver.create_backup(sg_client, backup=backup)
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
                self._finish_create_backup(SYNC_FAILED, volume, backup)

    def _finish_delete_backup(self, result, backup, status=None):
        if result == SYNC_SUCCEED:
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
            self._finish_delete_backup(SYNC_FAILED, backup)
            raise exception.InvalidBackup(reason=msg)

        try:
            if backup.volume_id is None:
                self._finish_delete_backup(SYNC_SUCCEED, backup)
                return
            sg_client = SGClientObject(backup.sg_client)
            driver_backup = self.driver.get_backup(sg_client, backup)
            if driver_backup['status'] == fields.BackupStatus.DELETED:
                self._finish_delete_backup(SYNC_SUCCEED, backup)
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
                self._finish_delete_backup(SYNC_FAILED, backup)

    def _finish_restore_backup(self, result, backup, volume):
        if result == SYNC_SUCCEED:
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

    def _do_restore_backup(self, context, backup, volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            sg_client = SGClientObject(volume.sg_client)
            cinder_client = self._create_cinder_client(context)
            self._attach_volume_to_sg(context, logical_volume_id, sg_client)
            cinder_volume = self._wait_cinder_volume_status(
                cinder_client, logical_volume_id, 'in-use')
            if cinder_volume.status != 'in-use':
                self._finish_restore_backup(SYNC_FAILED, backup, volume)
                return
            mountpoint = cinder_volume.attachments[0]['device']
            device = self._get_attach_device(sg_client, mountpoint)
            # TODO(luobin): current restore is a synchronization function
            self.driver.restore_backup(sg_client, backup, volume, device)
            self._detach_volume_from_sg(context, logical_volume_id,
                                        sg_client)
            cinder_volume = self._wait_cinder_volume_status(
                cinder_client, logical_volume_id, 'available')
            if cinder_volume.status == 'available':
                self._finish_restore_backup(SYNC_SUCCEED, backup, volume)
            else:
                self._finish_restore_backup(SYNC_FAILED, backup, volume)
        except Exception as err:
            msg = (_LE("Restore backup:%(backup_id)s failed, "
                       "err:%(err)s."),
                   {"backup_id": backup.id, "err": err})
            LOG.error(msg)
            self._finish_restore_backup(SYNC_FAILED, backup, volume)

    def restore_backup(self, context, backup_id, volume_id):
        LOG.info(_LI("Restore backup started, backup:%(backup_id)s, volume: "
                     "%(volume_id)s"),
                 {'volume_id': volume_id, 'backup_id': backup_id})
        backup = objects.Backup.get_by_id(context, backup_id)
        volume = objects.Volume.get_by_id(context, volume_id)
        volume.update({'sg_client': backup.sg_client})
        volume.save()

        expected_status = 'restoring'
        actual_status = backup['status']
        if actual_status != expected_status:
            msg = (_LE('Restore backup aborted, expected backup status '
                       '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_restore_backup(SYNC_FAILED, backup, volume)
            raise exception.InvalidBackup(reason=msg)

        self._do_restore_backup(context, backup, volume)

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
            "availability_zone": availability_zone,
            "size": backup['size']
        }
        return backup_record

    def import_record(self, context, backup_id, backup_record):
        LOG.info(_LI('Import record started, backup_record: %s.'),
                 backup_record)
        backup = objects.Backup.get_by_id(context, backup_id)
        backup_type = backup_record.get('backup_record', constants.FULL_BACKUP)
        driver_data = jsonutils.dumps(backup_record.get('driver_data'))
        size = backup_record.get('size', 1)
        sg_client = SGClientObject()
        backup.update({'type': backup_type,
                       'driver_data': driver_data,
                       'status': fields.BackupStatus.AVAILABLE,
                       'size': size,
                       'sg_client': sg_client.dumps()})
        backup.save()

    def _update_snapshot_error(self, snapshot):
        snapshot.update({'status': fields.SnapshotStatus.ERROR})
        snapshot.save()

    def _finish_create_snapshot(self, result, snapshot):
        if result == SYNC_SUCCEED:
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
        snapshot.update({'sg_client': volume.sg_client})
        snapshot.save()

        expected_status = fields.SnapshotStatus.CREATING
        actual_status = snapshot['status']
        if actual_status != expected_status:
            msg = (_('Create snapshot aborted, expected snapshot status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            LOG.error(msg)
            self._finish_create_snapshot(SYNC_FAILED, snapshot)
            raise exception.InvalidSnapshot(reason=msg)

        try:
            sg_client = SGClientObject(snapshot.sg_client)
            self.driver.create_snapshot(sg_client, snapshot=snapshot,
                                        volume=volume)
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
        if result == SYNC_SUCCEED:
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
            self._finish_delete_snapshot(SYNC_FAILED, snapshot)
            raise exception.InvalidBackup(reason=msg)

        try:
            sg_client = SGClientObject(snapshot.sg_client)
            driver_snapshot = self.driver.get_snapshot(sg_client,
                                                       snapshot=snapshot)
            LOG.info(_LI("driver snapshot info: %s"), driver_snapshot)
            if driver_snapshot['status'] == fields.SnapshotStatus.DELETED:
                self._finish_delete_snapshot(SYNC_SUCCEED, snapshot)
                return
            self.driver.delete_snapshot(sg_client, snapshot=snapshot)
            self.sync_snapshots[snapshot_id] = {
                'action': 'delete',
                'snapshot': snapshot
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("delete snapshot:%(snapshot_id)s failed, "
                              "err:%(err)s"),
                          {"snapshot_id": snapshot_id, "err": err})
                self._finish_delete_snapshot(SYNC_FAILED, snapshot)

    def _finish_rollback_snapshot(self, result, snapshot, volume, status=None):
        if result == SYNC_SUCCEED:
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
            self._finish_rollback_snapshot(SYNC_FAILED, snapshot, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            sg_client = SGClientObject(volume.sg_client)
            self.driver.rollback_snapshot(sg_client, snapshot)
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
                self._finish_rollback_snapshot(SYNC_FAILED, snapshot, volume)

    def _finish_enable_replicate(self, result, volume, status=None):
        if result == SYNC_SUCCEED:
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
            self._finish_enable_replicate(SYNC_FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            sg_client = SGClientObject(volume.sg_client)
            self.driver.create_replicate(sg_client, volume=volume)
            self.sync_replicates[volume_id] = {
                'action': 'create_replicate',
                'volume': volume
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("create volume:%(volume_id)s replicate failed, "
                              "err:%(err)s"),
                          {"volume_id": volume_id, "err": err})
                self._finish_enable_replicate(SYNC_FAILED, volume)

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
            self._finish_enable_replicate(SYNC_FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            sg_client = SGClientObject(volume.sg_client)
            self.driver.enable_replicate(sg_client, volume=volume)
            self.sync_replicates[volume_id] = {
                'action': 'enable_replicate',
                'volume': volume
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("enable volume:%(volume_id)s replicate failed, "
                              "err:%(err)s"),
                          {"volume_id": volume_id, "err": err})
                self._finish_enable_replicate(SYNC_FAILED, volume)

    def _finish_disable_replicate(self, result, volume, status=None):
        if result == SYNC_SUCCEED:
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
            self._finish_disable_replicate(SYNC_FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            sg_client = SGClientObject(volume.sg_client)
            self.driver.disable_replicate(sg_client, volume=volume)
            self.sync_replicates[volume_id] = {
                'action': 'disable_replicate',
                'volume': volume
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("disable volume:%(volume_id)s replicate failed, "
                              "err:%(err)s"),
                          {"volume_id": volume_id, "err": err})
                self._finish_disable_replicate(SYNC_FAILED, volume)

    def _finish_delete_replicate(self, result, volume, status=None):
        if result == SYNC_SUCCEED:
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
            self._finish_delete_replicate(SYNC_FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            sg_client = SGClientObject(volume.sg_client)
            self.driver.delete_replicate(sg_client, volume=volume)
            self.sync_replicates[volume_id] = {
                'action': 'delete_replicate',
                'volume': volume
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("delete volume:%(volume_id)s replicate failed, "
                              "err:%(err)s"),
                          {"volume_id": volume_id, "err": err})
                self._finish_delete_replicate(SYNC_FAILED, volume)

    def _finish_failover_replicate(self, result, volume, status=None):
        if result == SYNC_SUCCEED:
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
            self._finish_failover_replicate(SYNC_FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            sg_client = SGClientObject(volume.sg_client)
            self.driver.failover_replicate(sg_client, volume=volume,
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
                self._finish_failover_replicate(SYNC_FAILED, volume)

    def _finish_reverse_replicate(self, result, volume, driver_volume=None):
        if result == SYNC_SUCCEED:
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
            self._finish_reverse_replicate(SYNC_FAILED, volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            sg_client = SGClientObject(volume.sg_client)
            self.driver.reverse_replicate(sg_client, volume=volume)
            self.sync_replicates[volume_id] = {
                'action': 'reverse_replicate',
                'volume': volume
            }
        except exception.SGDriverError as err:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("reverse volume:%(volume_id)s replicate "
                              "failed, err:%(err)s"),
                          {"volume_id": volume_id, "err": err})
                self._finish_reverse_replicate(SYNC_FAILED, volume)

    def _finish_create_volume(self, result, volume, snapshot):
        if result == SYNC_SUCCEED:
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

    def _do_create_volume(self, context, snapshot, volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            # wait cinder volume to be available
            cinder_client = self._create_cinder_client(context)
            cinder_volume = self._wait_cinder_volume_status(
                cinder_client, logical_volume_id, 'available')
            if cinder_volume.status != 'available':
                self._finish_create_volume(SYNC_FAILED, volume, snapshot)
                return
            # attach cinder volume to sg_client
            sg_client = SGClientObject(volume.sg_client)
            self._attach_volume_to_sg(context, logical_volume_id, sg_client)
            cinder_volume = self._wait_cinder_volume_status(
                cinder_client, logical_volume_id, 'in-use')
            if cinder_volume.status != 'in-use':
                self._finish_create_volume(SYNC_FAILED, volume, snapshot)
                return
            mountpoint = cinder_volume.attachments[0]['device']
            # copy snapshot to device
            device = self._get_attach_device(sg_client, mountpoint)
            driver_volume = self.driver.query_volume_from_snapshot(
                sg_client, volume.id)
            if driver_volume['status'] == fields.VolumeStatus.DELETED:
                self.driver.create_volume_from_snapshot(
                    sg_client, snapshot, volume.id, device)
            # wait vol available
            self._wait_vols_from_snap(volume)
            self._finish_create_volume(SYNC_SUCCEED, volume, snapshot)
        except Exception:
            self._finish_create_volume(SYNC_FAILED, volume, snapshot)

    def create_volume(self, context, snapshot_id, volume_id):
        LOG.info(_LI("Create new volume from snapshot, snapshot_id %s"),
                 snapshot_id)
        snapshot = objects.Snapshot.get_by_id(context, snapshot_id)
        volume = objects.Volume.get_by_id(context, volume_id)
        volume.update({'sg_client': snapshot.sg_client})
        volume.save()
        self._do_create_volume(context, snapshot, volume)

    @retry(wait_fixed=CONF.sync_status_interval)
    def _wait_cinder_volume_status(self, cinder_client, volume_id, status):
        cinder_volume = cinder_client.volumes.get(volume_id)
        if 'error' not in cinder_volume.status:
            if cinder_volume.status != status:
                raise Exception("Volume status is not %s" % status)
        return cinder_volume

    @retry(wait_fixed=CONF.sync_status_interval)
    def _wait_vols_from_snap(self, volume):
        driver_volume = self.driver.query_volume_from_snapshot(volume.id)
        if driver_volume['status'] != fields.VolumeStatus.ENABLED:
            raise Exception("Volume is creating")
        return driver_volume
