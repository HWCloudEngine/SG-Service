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

from sgservice.common.clients import cinder
from sgservice.common.clients import nova
from sgservice.common import constants
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
               default=10),
    cfg.PortOpt('sgs_agent_port',
                default=7127,
                help='The port of sgs agent'),
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
    fields.SnapshotStatus.ROLLING_BACK: 'rollback'
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
            if cinder_volume.status == 'available':
                return self.enable_sg(context, volume.id)
            elif (cinder_volume.status == 'in-use'
                  and len(cinder_volume.attachments) == 1
                  and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                driver_volume = self.driver.get_volume(sg_client, volume)
                if driver_volume['status'] == fields.VolumeStatus.ENABLED:
                    return self._finish_enable_sg(SYNC_SUCCEED, volume)
            elif (cinder_volume.status == 'attaching'
                  and len(cinder_volume.attachments) == 1
                  and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'in-use')
                # as we can't get the real mountpoint in sg-client,
                # set enable failed
                return self._finish_enable_sg(SYNC_FAILED, volume)
            # TODO(smile-luobin): other status
            return self._finish_enable_sg(SYNC_FAILED, volume)
        except Exception:
            return self._finish_enable_sg(SYNC_FAILED, volume)

    def _init_deleting_volume(self, context, volume):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            sg_client = SGClientObject(volume.sg_client)
            if (cinder_volume.status == 'in-use'
                and len(cinder_volume.attachments) == 1
                and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                return self.disable_sg(context, volume.id)
            elif (cinder_volume.status == 'detaching'
                    and len(cinder_volume.attachments) == 1
                    and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'available')
                if cinder_volume.status == 'available':
                    return self._finish_delete_volume(SYNC_SUCCEED, volume)
                else:
                    return self._finish_delete_volume(SYNC_FAILED, volume)
            elif cinder_volume.status == 'available':
                return self._finish_delete_volume(SYNC_SUCCEED, volume)
            # TODO(smile-luobin): other status
            return self._finish_delete_volume(SYNC_FAILED, volume)
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
            # cinder volume still is creating or available
            if cinder_volume.status in ['creating', 'available']:
                return self.create_volume(context, snapshot.id, volume.id)
            elif (cinder_volume.status in ['in-use', 'attaching']
                    and len(cinder_volume.attachments) == 1
                    and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'in-use')
                # as we can't get the real mountpoint, set create failed
                return self._finish_create_volume(SYNC_FAILED,
                                                  volume, snapshot)
            elif (cinder_volume.status in ['detaching']
                    and len(cinder_volume.attachments) == 1
                    and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'available')
                if cinder_volume.status == "available":
                    return self._finish_create_volume(SYNC_SUCCEED,
                                                      volume, snapshot)
                else:
                    return self._finish_create_volume(SYNC_FAILED, volume,
                                                      snapshot)
            # TODO(smile-luobin): other status
            return self._finish_create_volume(SYNC_FAILED, volume, snapshot)
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
            sg_client = SGClientObject(volume.sg_client)
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            # cinder volume still available
            if cinder_volume.status == 'available':
                return self._do_restore_backup(context, backup, volume)
            elif (cinder_volume.status in ['in-use', 'attaching']
                    and len(cinder_volume.attachments) == 1
                    and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'in-use')
                # as we can't get the real mountpoint, set restore failed
                return self._finish_restore_backup(SYNC_FAILED, backup, volume)
            elif (cinder_volume.status in ['detaching']
                    and len(cinder_volume.attachments) == 1
                    and cinder_volume.attachments[0]['server_id']
                    == sg_client.instance):
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'available')
                if cinder_volume.status == "available":
                    return self._finish_restore_backup(SYNC_SUCCEED, backup,
                                                       volume)
                else:
                    return self._finish_restore_backup(SYNC_FAILED, backup,
                                                       volume)
            # TODO(smile-luobin): other status
            return self._finish_restore_backup(SYNC_FAILED, backup, volume)
        except Exception:
            self._finish_restore_backup(SYNC_FAILED, backup, volume)

    def _init_attaching_volume(self, context, volume):
        attachment = objects.VolumeAttachmentList.get_all_by_volume_id(
            context, volume.id)[-1]
        if CONF.sg_client.sg_client_mode == constants.ISCSI_MODE:
            self._do_iscsi_attach_volume(volume, attachment)
        else:
            if 'logicalVolumeId' in volume.metadata:
                logical_volume_id = volume.metadata['logicalVolumeId']
            else:
                logical_volume_id = volume.id
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            sg_client = SGClientObject(volume.sg_client)
            if (sg_client.instance == CONF.sg_client.sg_client_instance
                    and cinder_volume.status == 'available'):
                return self._do_agent_attach_volume(context, volume,
                                                    attachment)
            elif cinder_volume.status in 'attaching':
                self._wait_cinder_volume_status(cinder_client,
                                                logical_volume_id, 'in-use')
            elif cinder_volume.status == 'detaching':
                self._wait_cinder_volume_status(cinder_client,
                                                logical_volume_id, 'available')
            # other status, we can't get real mountpoint, set attach failed
            return self._finish_attach_volume(SYNC_FAILED, volume, attachment)

    def _init_detaching_volume(self, context, volume):
        attachment = objects.VolumeAttachmentList.get_all_by_volume_id(
            context, volume.id)[-1]
        if CONF.sg_client.sg_client_mode == constants.ISCSI_MODE:
            self._do_iscsi_detach_volume(volume, attachment)
        else:
            if 'logicalVolumeId' in volume.metadata:
                logical_volume_id = volume.metadata['logicalVolumeId']
            else:
                logical_volume_id = volume.id
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            sg_client = SGClientObject(volume.sg_client)
            if sg_client.instance == attachment.logical_instance_id:
                if cinder_volume.status == 'in-use':
                    return self._do_agent_detach_volume(context, volume,
                                                        attachment)
                elif cinder_volume.status in ['in-use', 'detaching']:
                    if cinder_volume.status == 'in-use':
                        self._detach_volume_from_sg(context, logical_volume_id,
                                                    sg_client)
                    cinder_volume = self._wait_cinder_volume_status(
                        cinder_client, logical_volume_id, 'available')
                elif cinder_volume.status != 'available':
                    # TODO(smile-luobin): other status
                    return self._finish_detach_volume(SYNC_FAILED, volume,
                                                      attachment)
            sg_client = SGClientObject()
            if sg_client.dumps() != volume.sg_client:
                volume.update({'sg_client': sg_client.dumps()})
                volume.save()
            if cinder_volume.status == 'available':
                device = self._attach_volume_to_sg(
                    context, logical_volume_id, sg_client)
                self.driver.terminate_connection(
                    sg_client, volume, constants.AGENT_MODE, device)
            elif cinder_volume.status == 'attaching':
                self._wait_cinder_volume_status(
                    cinder_client, logical_volume_id, 'in-use')
                # as we can't get the real mountpoint, set detach failed
                return self._finish_detach_volume(
                    SYNC_FAILED, volume, attachment, fields.VolumeStatus.ERROR)
            # other status
            else:
                return self._finish_detach_volume(
                    SYNC_FAILED, volume, attachment, fields.VolumeStatus.ERROR)

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
                    try:
                        volume = objects.Volume.get_by_id(
                            context, backup['restore_volume_id'])
                        self._init_restoring_volume(context, volume, backup)
                    except Exception:
                        backup.update(
                            {'status': fields.BackupStatus.AVAILABLE})
                        backup.save()
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
        @utils.synchronized(sg_client.instance)
        def _do_attach_volume(context, volume_id, sg_client):
            try:
                old_devices = self.driver.list_devices(sg_client)
                LOG.info(_LI("old devices: %s"), old_devices)
                nova_client = self._create_nova_client(self.admin_context)
                nova_client.volumes.create_server_volume(sg_client.instance,
                                                         volume_id)
                cinder_client = self._create_cinder_client(context)
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, volume_id, 'in-use')
                if cinder_volume.status != 'in-use':
                    raise Exception(_LE("attach volume to sg failed"))
                new_devices = self.driver.list_devices(sg_client)
                LOG.info(_LI("new devices: %s"), new_devices)
                added_devices = [device for device in new_devices
                                 if device not in old_devices]
                return added_devices[0]
            except Exception as err:
                LOG.error(err)
                raise exception.AttachSGFailed(reason=err)

        return _do_attach_volume(context, volume_id, sg_client)

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
            sg_client = SGClientObject(backup.sg_client)
            try:
                driver_backup = self.driver.get_backup(sg_client, backup)
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
                # TODO(luobin): current restore backup is synchronized func
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
            sg_client = SGClientObject(snapshot.sg_client)
            try:
                driver_snapshot = self.driver.get_snapshot(sg_client, snapshot)
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
            sg_client = SGClientObject(volume.sg_client)
            try:
                driver_volume = self.driver.get_volume(sg_client, volume)
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

    # detach volume from sg-client
    def _detach_volume_from_sg(self, context, volume_id, sg_client):
        @utils.synchronized(sg_client.instance)
        def _do_detach_volume(context, volume_id, sg_client):
            try:
                nova_client = self._create_nova_client(self.admin_context)
                nova_client.volumes.delete_server_volume(sg_client.instance,
                                                         volume_id)
                cinder_client = self._create_cinder_client(context)
                cinder_volume = self._wait_cinder_volume_status(
                    cinder_client, volume_id, 'available')
                if cinder_volume.status != 'available':
                    raise Exception(_LE("detach volume from sg failed"))
            except Exception as err:
                LOG.error(err)
                raise exception.DetachSGFailed(reason=err)

        _do_detach_volume(context, volume_id, sg_client)

    def _sync_vols_from_snap(self):
        for volume_id, volume_info in self.sync_vols_from_snap.items():
            volume = volume_info['volume']
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
            device = self._attach_volume_to_sg(context, logical_volume_id,
                                               sg_client)
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
                driver_volume = self.driver.get_volume(sg_client, volume)
                if driver_volume['status'] != fields.VolumeStatus.DELETED:
                    self.driver.disable_sg(sg_client, volume)
                cinder_volume = cinder_client.volumes.get(logical_volume_id)
                if (cinder_volume.status == 'in-use'
                    and len(cinder_volume.attachments) == 1
                    and cinder_volume.attachments[0]['server_id']
                        == sg_client.instance):
                    self._detach_volume_from_sg(context, logical_volume_id,
                                                sg_client)
                    self._finish_delete_volume(SYNC_SUCCEED, volume)
                else:
                    self._finish_delete_volume(SYNC_SUCCEED, volume)
            else:
                self._finish_delete_volume(SYNC_SUCCEED, volume)
        except cinder_exc.NotFound:
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
        try:
            # step 1: call initialize_connection request to sg-client
            LOG.debug(_LI("call initialize connection request"))
            sg_client = SGClientObject(volume.sg_client)
            self.driver.initialize_connection(sg_client, volume,
                                              constants.AGENT_MODE)
            # step 2: detach from system sg_client
            LOG.debug(_LI('detach volume from system sg-client'))
            self._detach_volume_from_sg(context, logical_volume_id,
                                        sg_client)
            # step 3: attach volume to guest vm
            LOG.debug(_LI('attach volume to guest vm'))
            instance_id = attachment.logical_instance_id
            instance_host = attachment.instance_host
            sg_client = SGClientObject(instance=instance_id,
                                       host=instance_host)
            volume.update({'sg_client': sg_client.dumps()})
            volume.save()
            device = self._attach_volume_to_sg(context, logical_volume_id,
                                               sg_client)
            # step 4: call attach volume request to sg-client
            self.driver.attach_volume(sg_client, volume, device,
                                      attachment.instance_host)
            driver_data = {'driver_type': constants.AGENT_MODE}
            volume.update({'driver_data': jsonutils.dumps(driver_data)})
            volume.save()
            return self._finish_attach_volume(SYNC_SUCCEED, volume, attachment,
                                              device)
        except Exception as err:
            LOG.error(_LE("attach agent mode volume failed, err:%s"), err)
            return self._rollback_agent_attach_volume(context, volume,
                                                      attachment)

    def _do_iscsi_attach_volume(self, volume, attachment):
        # initialize connection
        try:
            sg_client = SGClientObject(volume.sg_client)
            connection_info = self.driver.initialize_connection(
                sg_client, volume, constants.ISCSI_MODE)
        except Exception as err:
            LOG.error(_LE("attach iscsi mode volume failed, err:%s"), err)
            return self._finish_attach_volume(
                SYNC_FAILED, volume, attachment,
                status=fields.VolumeStatus.ENABLED)
        # use iscsi agent attach volume
        try:
            instance_host = attachment.instance_host
            sgs_agent = AgentClient(instance_host, CONF.sgs_agent_port)
            mountpoint = sgs_agent.connect_volume(connection_info)[
                'mountpoint']
        except Exception as err:
            LOG.error(_LE("attach iscsi mode volume failed, err:%s"), err)
            # terminate connection rollback attach
            self.driver.terminate_connection(sg_client, volume,
                                             constants.ISCSI_MODE)
            return self._finish_attach_volume(
                SYNC_FAILED, volume, attachment,
                status=fields.VolumeStatus.ENABLED)

        driver_data = {'driver_type': constants.ISCSI_MODE,
                       'connection_info': connection_info}
        volume.update({'driver_data': jsonutils.dumps(driver_data)})
        volume.save()
        self._finish_attach_volume(SYNC_SUCCEED, volume, attachment,
                                   mountpoint=mountpoint)

    def _rollback_agent_attach_volume(self, context, volume, attachment):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            sg_client = SGClientObject(volume.sg_client)
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            if sg_client.instance == attachment.logical_instance_id:
                # attach failed in step 4;
                if cinder_volume.status == 'in-use':
                    self.driver.detach_volume(sg_client, volume)
                    self._detach_volume_from_sg(context, logical_volume_id,
                                                sg_client)
                    cinder_volume = cinder_client.volumes.get(
                        logical_volume_id)
                # attach failed in step 3 and can't rollback; set error
                elif cinder_volume.status != 'available':
                    return self._finish_attach_volume(SYNC_FAILED, volume,
                                                      attachment)
                sg_client = SGClientObject()
                volume.update({'sg_client': sg_client.dumps()})
                volume.save()
            # attach failed in step3 and rollback
            if cinder_volume.status == 'available':
                device = self._attach_volume_to_sg(context, logical_volume_id,
                                                   sg_client)
                self.driver.terminate_connection(
                    sg_client, volume, constants.AGENT_MODE, device)
                return self._finish_attach_volume(
                    SYNC_FAILED, volume, attachment,
                    fields.VolumeStatus.ENABLED)
            # attach failed in step 2 and rollback; or step 1;
            elif cinder_volume.status == 'in-use':
                self.driver.terminate_connection(
                    sg_client, volume, constants.AGENT_MODE)
                return self._finish_attach_volume(
                    SYNC_FAILED, volume, attachment,
                    fields.VolumeStatus.ENABLED)
            # attach failed in step 2 and can't rollback, set error
            else:
                return self._finish_attach_volume(SYNC_FAILED, volume,
                                                  attachment)
        except Exception as err:
            LOG.error(_LE("rollback agent attach volume failed, error:%s"),
                      err)
            self._finish_attach_volume(SYNC_FAILED, volume, attachment)

    def _finish_attach_volume(self, sync_result, volume, attachment,
                              mountpoint=None,
                              status=fields.VolumeStatus.ERROR_ATTACHING):
        if sync_result == SYNC_SUCCEED:
            LOG.info(_LI("attach volume:%s succeed"), volume.id)
            attachment.finish_attach(mountpoint)
        else:
            LOG.info(_LI("attach volume:%s failed"), volume.id)
            attachment.destroy()
            volume.status = status
            volume.save()

    def attach_volume(self, context, volume_id, attachment_id):
        attachment = objects.VolumeAttachment.get_by_id(context, attachment_id)
        instance_uuid = attachment.instance_uuid
        LOG.info(_LI("Attach volume '%(vol_id)s' to '%(instance_id)s'"),
                 {"vol_id": volume_id, "instance_id": instance_uuid})
        volume = objects.Volume.get_by_id(context, volume_id)
        if attachment.instance_host is None:
            msg = (_LE("Attach volume failed, can't get instance host"))
            LOG.error(msg)
            return self._finish_attach_volume(SYNC_FAILED, volume, attachment)

        try:
            if CONF.sg_client.sg_client_mode == 'iscsi':
                self._do_iscsi_attach_volume(volume, attachment)
            else:
                self._do_agent_attach_volume(context, volume, attachment)
        except Exception as err:
            msg = (_LE("Attach volume failed, err:%s"), err)
            LOG.error(msg)
            self._finish_attach_volume(SYNC_FAILED, volume, attachment)

    def _do_iscsi_detach_volume(self, volume, attachment):
        driver_data = jsonutils.loads(volume.driver_data)
        connection_info = driver_data['connection_info']
        instance_host = attachment.instance_host
        try:
            sgs_agent = AgentClient(instance_host, CONF.sgs_agent_port)
            sgs_agent.disconnect_volume(connection_info)
            sg_client = SGClientObject(volume.sg_client)
            self.driver.terminate_connection(sg_client, volume,
                                             constants.ISCSI_MODE)
            self._finish_detach_volume(SYNC_SUCCEED, volume, attachment)
        except Exception:
            self._finish_detach_volume(SYNC_FAILED, volume, attachment)

    def _do_agent_detach_volume(self, context, volume, attachment):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            # step 1: call detach volume request in guest vm sg-client
            sg_client = SGClientObject(volume.sg_client)
            LOG.debug(_LI("call detach volume request to sg-client"))
            self.driver.detach_volume(sg_client, volume)
            # step 2: detach volume from guest vm
            LOG.debug(_LE('detach volume from guest vm'))
            self._detach_volume_from_sg(context, logical_volume_id,
                                        sg_client)
            # step 3: attach volume to system sg_client
            LOG.debug(_LE('detach volume from guest vm'))
            sg_client = SGClientObject()
            volume.update({'sg_client': sg_client.dumps()})
            volume.save()
            device = self._attach_volume_to_sg(context, logical_volume_id,
                                               sg_client)
            # step 4: call terminate connection request in system sg-client
            LOG.debug(_LE('call terminate connection request to sg-client'))
            self.driver.terminate_connection(sg_client, volume,
                                             constants.AGENT_MODE, device)
            self._finish_detach_volume(SYNC_SUCCEED, volume, attachment)
        except Exception as err:
            LOG.error(_LE("agent mode detach volume failed, err:%s"), err)
            return self._rollback_agent_detach_volume(context, volume,
                                                      attachment)

    def _rollback_agent_detach_volume(self, context, volume, attachment):
        if 'logicalVolumeId' in volume.metadata:
            logical_volume_id = volume.metadata['logicalVolumeId']
        else:
            logical_volume_id = volume.id
        try:
            sg_client = SGClientObject(volume.sg_client)
            cinder_client = self._create_cinder_client(context)
            cinder_volume = cinder_client.volumes.get(logical_volume_id)
            if sg_client.instance == CONF.sg_client.sg_client_instance:
                if cinder_volume.status == 'in-use':
                    # detach failed in step 4;
                    self.driver.initialize_connection(
                        sg_client, volume, constants.AGENT_MODE)
                    self._detach_volume_from_sg(context, logical_volume_id,
                                                sg_client)
                    cinder_volume = cinder_client.volumes.get(
                        logical_volume_id)
                # detach failed in step 3 and can't rollback, set error
                elif cinder_volume.status != 'available':
                    return self._finish_detach_volume(SYNC_FAILED,
                                                      volume, attachment)
                instance_id = attachment.logical_instance_id
                instance_host = attachment.instance_host
                sg_client = SGClientObject(instance=instance_id,
                                           host=instance_host)
                volume.update({'sg_client': sg_client.dumps()})
                volume.save()
            # detach failed in step 3;
            if cinder_volume.status == 'available':
                device = self._attach_volume_to_sg(context, logical_volume_id,
                                                   sg_client)
                self.driver.attach_volume(sg_client, volume, device)
                driver_data = {'driver_type': constants.AGENT_MODE}
                volume.update({'driver_data': jsonutils.dumps(driver_data)})
                return self._finish_attach_volume(SYNC_SUCCEED, volume,
                                                  attachment, device)
            # detach failed in step 1;
            elif cinder_volume.status == 'in-use':
                self.driver.attach_volume(sg_client, volume, attachment.device)
                return self._finish_attach_volume(SYNC_SUCCEED, volume,
                                                  attachment,
                                                  attachment.device)
            # detach failed in step 2 and can't rollback, set error
            else:
                return self._finish_detach_volume(SYNC_FAILED,
                                                  volume, attachment)
        except Exception:
            return self._finish_detach_volume(SYNC_FAILED,
                                              volume, attachment)

    def _finish_detach_volume(self, sync_result, volume, attachment,
                              status=fields.VolumeStatus.ERROR_DETACHING):
        if sync_result == SYNC_SUCCEED:
            volume.finish_detach(attachment.id)
        else:
            attachment.destroy()
            volume.update({'status': status})
            volume.save()

    def detach_volume(self, context, volume_id, attachment_id):
        attachment = objects.VolumeAttachment.get_by_id(context, attachment_id)
        instance_uuid = attachment.instance_uuid
        LOG.info(_LI("Detach volume '%(vol_id)s' from '%(instance_id)s'"),
                 {"vol_id": volume_id, "instance_id": instance_uuid})
        volume = objects.Volume.get_by_id(context, volume_id)

        try:
            driver_data = jsonutils.loads(volume.driver_data)
            driver_type = driver_data['driver_type']
            if driver_type == 'iscsi':
                self._do_iscsi_detach_volume(volume, attachment)
            else:
                self._do_agent_detach_volume(context, volume, attachment)
        except Exception as err:
            msg = (_LE("Attach volume failed, err:%s"), err)
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
            self.driver.delete_backup(sg_client, backup=backup)
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
            LOG.info(_LI('restore backup succeed, backup_id: %s'),
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
            device = self._attach_volume_to_sg(context, logical_volume_id,
                                               sg_client)
            # TODO(luobin): current restore is a synchronization function
            self.driver.restore_backup(sg_client, backup, volume, device)
            self._detach_volume_from_sg(context, logical_volume_id,
                                        sg_client)
            self._finish_restore_backup(SYNC_SUCCEED, backup, volume)
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
            "backup_size": backup['size']
        }
        return backup_record

    def import_record(self, context, backup_id, backup_record):
        LOG.info(_LI('Import record started, backup_record: %s.'),
                 backup_record)
        backup = objects.Backup.get_by_id(context, backup_id)
        backup_type = backup_record.get('backup_type', constants.FULL_BACKUP)
        driver_data = jsonutils.dumps(backup_record.get('driver_data'))
        size = backup_record.get('backup_size', 1)
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
            LOG.info(_LI("rollback snapshot:%s succeed."), snapshot.id)
            snapshot.destroy()
            volume.update({'status': volume.previous_status})
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
            device = self._attach_volume_to_sg(context, logical_volume_id,
                                               sg_client)
            self.driver.create_volume_from_snapshot(sg_client, snapshot,
                                                    volume.id, device)
            # wait vol available
            self._wait_vols_from_snap(volume)
            # detach volume from sg-client
            self._detach_volume_from_sg(context, logical_volume_id, sg_client)
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

    @retry(wait_fixed=CONF.sync_status_interval * 1000,
           retry_on_exception=exception.ErrorStatus)
    def _wait_cinder_volume_status(self, cinder_client, volume_id, status):
        cinder_volume = cinder_client.volumes.get(volume_id)
        if 'error' not in cinder_volume.status:
            if cinder_volume.status != status:
                reason = (_LI("Volume status is %s"), cinder_volume.status)
                LOG.info(reason)
                raise exception.ErrorStatus(reason=reason)
        return cinder_volume

    @retry(wait_fixed=CONF.sync_status_interval * 1000,
           retry_on_exception=exception.ErrorStatus)
    def _wait_vols_from_snap(self, volume):
        sg_client = SGClientObject(volume.sg_client)
        driver_volume = self.driver.query_volume_from_snapshot(sg_client,
                                                               volume.id)
        if driver_volume['status'] != fields.VolumeStatus.ENABLED:
            reason = (_LI("Volume status is %s"), driver_volume['status'])
            LOG.info(reason)
            raise exception.ErrorStatus(reason=reason)
        return driver_volume
