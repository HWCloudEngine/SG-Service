# Copyright 2011 OpenStack Foundation
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
import oslo_messaging as messaging
from oslo_utils import excutils
from oslo_utils import uuidutils
from oslo_service import loopingcall
from oslo_serialization import jsonutils

from sgsclient.exceptions import NotFound
from sgservice.i18n import _, _LI, _LE
from sgservice.common import constants
from sgservice.common.clients import cinder
from sgservice.common.clients import nova
from sgservice.common.clients import sgs
from sgservice.controller.api import API as ServiceAPI
from sgservice import exception
from sgservice import objects
from sgservice.objects import fields
from sgservice import manager
from sgservice import utils
from sgservice import context as sg_context

proxy_manager_opts = [
    cfg.IntOpt('sync_interval',
               default=5,
               help='seconds between cascading and cascaded sgservices when '
                    'synchronizing data'),
    cfg.IntOpt('status_query_count',
               default=5,
               help='status query times'),
    cfg.IntOpt('sync_status_interval',
               default=60,
               help='sync resources status interval')
]

REPLICATION_STATUS_ACTION_MAPPING = {
    fields.ReplicationStatus.CREATING: 'create',
    fields.ReplicationStatus.ENABLING: 'enable',
    fields.ReplicationStatus.DISABLING: 'disable',
    fields.ReplicationStatus.DELETING: 'delete',
    fields.ReplicationStatus.FAILING_OVER: 'failover',
    fields.ReplicationStatus.REVERSING: 'reverse'
}

REPLICATE_STATUS_ACTION_MAPPING = {
    fields.ReplicateStatus.CREATING: 'create',
    fields.ReplicateStatus.ENABLING: 'enable',
    fields.ReplicateStatus.DISABLING: 'disable',
    fields.ReplicateStatus.DELETING: 'delete',
    fields.ReplicateStatus.FAILING_OVER: 'failover',
    fields.ReplicateStatus.REVERSING: 'reverse'
}

VOLUME_STATUS_ACTION_MAPPING = {
    fields.VolumeStatus.ATTACHING: 'attach',
    fields.VolumeStatus.DETACHING: 'detach',
    fields.VolumeStatus.BACKING_UP: 'backup',
    fields.VolumeStatus.ENABLING: 'enable',
    fields.VolumeStatus.DISABLING: 'disable',
    fields.VolumeStatus.ROLLING_BACK: 'rollback',
    fields.VolumeStatus.RESTORING_BACKUP: 'restore',
    fields.VolumeStatus.DELETING: 'delete',
    fields.VolumeStatus.CREATING: 'create'
}

BACKUP_STATUS_ACTION_MAPPING = {
    fields.BackupStatus.CREATING: 'create',
    fields.BackupStatus.DELETING: 'delete',
    fields.BackupStatus.RESTORING: 'restore'
}

SNAPSHOT_STATUS_ACTION_MAPPING = {
    fields.SnapshotStatus.CREATING: 'create',
    fields.SnapshotStatus.DELETING: 'delete',
    fields.SnapshotStatus.ROLLING_BACK: 'rollback'
}

CHECKPOINT_STATUS_ACTION_MAPPING = {
    fields.CheckpointStatus.CREATING: 'create',
    fields.CheckpointStatus.DELETING: 'delete',
    fields.CheckpointStatus.ROLLING_BACK: 'rollback'
}

CONF = cfg.CONF
LOG = logging.getLogger(__name__)
CONF.register_opts(proxy_manager_opts)


class SGServiceProxy(manager.Manager):
    RPC_API_VERSION = '1.0'
    target = messaging.Target(version=RPC_API_VERSION)

    def __init__(self, service_name=None, *args, **kwargs):
        super(SGServiceProxy, self).__init__(*args, **kwargs)
        self.service_api = ServiceAPI()
        self.adminCinderClient = self._get_cascaded_cinder_client()
        self.adminSGSClient = self._get_cascaded_sgs_client()
        self.adminNovaClient = self._get_cascaded_nova_client()
        self.admin_context = sg_context.get_admin_context()

        self.volumes_mapping_cache = {}
        self.backups_mapping_cache = {}
        self.snapshots_mapping_cache = {}
        self.replications_mapping_cache = {}

        self.sync_status_interval = CONF.sync_status_interval
        self.sync_volumes = {}
        sync_volumes_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_volumes_status)
        sync_volumes_loop.start(interval=self.sync_status_interval,
                                initial_delay=self.sync_status_interval)

        self.sync_backups = {}
        sync_backups_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_backups_status)
        sync_backups_loop.start(interval=self.sync_status_interval,
                                initial_delay=self.sync_status_interval)

        self.sync_snapshots = {}
        sync_snapshots_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_snapshots_status)
        sync_snapshots_loop.start(interval=self.sync_status_interval,
                                  initial_delay=self.sync_status_interval)

        self.sync_checkpoints = {}
        sync_checkpoints_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_checkpoints_status)
        sync_checkpoints_loop.start(interval=self.sync_status_interval,
                                    initial_delay=self.sync_status_interval)

        self.sync_replicates = {}
        sync_replicates_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_replicates_status)
        sync_replicates_loop.start(interval=self.sync_status_interval,
                                   initial_delay=self.sync_status_interval)

        self.sync_replications = {}
        sync_replications_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_replications_status)
        sync_replications_loop.start(interval=self.sync_status_interval,
                                     initial_delay=self.sync_status_interval)

    def init_host(self, **kwargs):
        """ list all ing-status objects(volumes, snapshot, backups);
            add to self.volumes_mapping_cache, backups_mapping_cache,
            snapshots_mapping_cache, and replicates_mapping;
            start looping call to sync volumes, backups, snapshots,
            and replicates status;
        """
        filters = {'availability_zone': CONF.availability_zone}
        volumes = objects.VolumeList.get_all(self.admin_context,
                                             filters=filters)
        for volume in volumes:
            if volume.status in VOLUME_STATUS_ACTION_MAPPING.keys():
                self.sync_volumes[volume.id] = {
                    'action': VOLUME_STATUS_ACTION_MAPPING[volume.status]}
            if (volume.replicate_status
                in REPLICATE_STATUS_ACTION_MAPPING.keys()):
                self.sync_replicates[volume.id] = {
                    'action': REPLICATE_STATUS_ACTION_MAPPING[
                        volume.replicate_status]}

        snapshots = objects.SnapshotList.get_all(self.admin_context,
                                                 filters=filters)
        for snapshot in snapshots:
            if snapshot.status in SNAPSHOT_STATUS_ACTION_MAPPING.keys():
                self.sync_snapshots[snapshot.id] = {
                    'action': SNAPSHOT_STATUS_ACTION_MAPPING[snapshot.status]}

        backups = objects.BackupList.get_all(self.admin_context,
                                             filters=filters)
        for backup in backups:
            if backup.status in BACKUP_STATUS_ACTION_MAPPING.keys():
                self.sync_backups[backup.id] = {
                    'action': BACKUP_STATUS_ACTION_MAPPING[backup.status]}

        replications = objects.ReplicationList.get_all(self.admin_context)
        for replication in replications:
            if replication.status in REPLICATION_STATUS_ACTION_MAPPING.keys():
                self.sync_replications[replication.id] = {
                    'action': REPLICATION_STATUS_ACTION_MAPPING[
                        replication.status]}

        checkpoints = objects.CheckpointList.get_all(self.admin_context)
        for checkpoint in checkpoints:
            if checkpoint.status in CHECKPOINT_STATUS_ACTION_MAPPING.keys():
                self.sync_checkpoints[checkpoint.id] = {
                    'action': CHECKPOINT_STATUS_ACTION_MAPPING[
                        checkpoint.status]}

    def _sync_volumes_status(self):
        """ sync cascaded volumes'(in volumes_mapping_cache) status;
            and update cascading volumes' status
        """
        for volume_id, item in self.sync_volumes.items():
            action = item['action']
            try:
                volume = objects.Volume.get_by_id(self.admin_context,
                                                  volume_id)
            except exception.VolumeNotFound:
                self.sync_volumes.pop(volume_id)
                continue
            # already in stable status
            if volume.status not in VOLUME_STATUS_ACTION_MAPPING.keys():
                self.sync_volumes.pop(volume_id)
                continue
            try:
                csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
                csd_volume = self.adminSGSClient.volumes.get(csd_volume_id)
                LOG.info(_LI('csd volume info: %s'), csd_volume)
                if (csd_volume.status
                    not in VOLUME_STATUS_ACTION_MAPPING.keys()):
                    volume.update({'status': csd_volume.status})
                    volume.save()
                    self.sync_volumes.pop(volume_id)
                if action == 'attach':
                    attachment = item['attachment']
                    if volume.status == fields.VolumeStatus.IN_USE:
                        attachment.finish_attach(
                            csd_volume.attachments[0]['device'])
                    else:
                        attachment.destroy()
                if action == 'detach':
                    attachment = item['attachment']
                    if volume.status == fields.VolumeStatus.ENABLED:
                        volume.finish_detach(attachment.id)
                    else:
                        attachment.destroy()
            except (exception.CascadedResourceNotFound, NotFound):
                if action in ['disable', 'delete']:
                    LOG.info(_LI("disable or delete volume %s finished "),
                             volume_id)
                    volume.destroy()
                    self.volumes_mapping_cache.pop(volume_id)
                    self.sync_volumes.pop(volume_id)
            except Exception as exc:
                LOG.error(_LE("Sync volume %(volume_id)s status failed, "
                              "error: %(err)s."),
                          {"volume_id": volume_id, "err": exc})

    def _sync_backups_status(self):
        """ sync cascaded backups'(in volumes_mapping_cache) status;
            update cascading backups' status
        """
        for backup_id, item in self.sync_backups.items():
            action = item['action']
            try:
                backup = objects.Backup.get_by_id(self.admin_context,
                                                  backup_id)
            except exception.BackupNotFound:
                self.sync_backups.pop(backup_id)
                continue
            # if already in stable status
            if backup.status not in BACKUP_STATUS_ACTION_MAPPING.keys():
                self.sync_backups.pop(backup_id)
                continue

            try:
                csd_backup_id = self._get_csd_backup_id(backup_id)
                csd_backup = self.adminSGSClient.backups.get(csd_backup_id)
                LOG.info(_LI('csd backup info: %s'), csd_backup)
                if (csd_backup.status
                        not in BACKUP_STATUS_ACTION_MAPPING.keys()):
                    backup.update({'status': csd_backup.status})
                    backup.save()
                    self.sync_backups.pop(backup_id)
            except (exception.CascadedResourceNotFound, NotFound):
                if action == 'delete':
                    LOG.info(_LI("delete backup %s finished "), backup_id)
                    backup.destroy()
                else:
                    backup.update({'status': fields.BackupStatus.ERROR})
                    backup.save()
                self.backups_mapping_cache.pop(backup_id)
                self.sync_backups.pop(backup_id)
            except Exception as exc:
                LOG.error(_LE("Sync backup %(backup_id)s status failed, "
                              "error: %(err)s."),
                          {"backup_id": backup_id, "err": exc})

    def _sync_snapshots_status(self):
        """ sync cascaded snapshots'(in volumes_mapping_cache) status;
            update cascading snapshots' status;
            # TODO update cascading checkpoints' status if needed;
        """
        for snapshot_id, item in self.sync_snapshots.items():
            action = item['action']
            try:
                snapshot = objects.Snapshot.get_by_id(self.admin_context,
                                                      snapshot_id)
            except exception.SnapshotNotFound:
                self.sync_snapshots.pop(snapshot_id)
                continue
            # already in stable status
            if snapshot.status not in SNAPSHOT_STATUS_ACTION_MAPPING.keys():
                self.sync_snapshots.pop(snapshot_id)
                continue

            try:
                csd_snapshot_id = self._get_csd_snapshot_id(snapshot_id)
                csd_snapshot = self.adminSGSClient.snapshots.get(
                    csd_snapshot_id)
                LOG.info(_LI('csd snapshot info: %s'), csd_snapshot)
                if (csd_snapshot.status
                        not in SNAPSHOT_STATUS_ACTION_MAPPING.keys()):
                    snapshot.update({'status': csd_snapshot.status})
                    snapshot.save()
                    self.sync_snapshots.pop(snapshot_id)
            except (exception.CascadedResourceNotFound, NotFound):
                if action in ['delete', 'rollback']:
                    LOG.info(_LI("delete snapshot %s finished "), snapshot_id)
                    snapshot.destroy()
                else:
                    snapshot.update({'status': fields.SnapshotStatus.ERROR})
                    snapshot.save()
                self.snapshots_mapping_cache.pop(snapshot_id)
                self.sync_snapshots.pop(snapshot_id)
            except Exception as exc:
                LOG.error(_LE("Sync snapshot %(snapshot_id)s status failed, "
                              "error: %(err)s."),
                          {"snapshot_id": snapshot_id, "err": exc})

    def _sync_replicates_status(self):
        """ sync cascaded volumes'(in volumes_mapping_cache) replicate-status;
            update cascading volumes' replicate-status;
        """
        for volume_id, item in self.sync_replicates.items():
            action = item['action']
            try:
                volume = objects.Volume.get_by_id(self.admin_context,
                                                  volume_id)
            except exception.VolumeNotFound:
                self.sync_replicates.pop(volume_id)
                continue
            if (volume.replicate_status
                    not in REPLICATE_STATUS_ACTION_MAPPING.keys()):
                self.sync_replicates.pop(volume_id)
                continue
            try:
                csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
                csd_volume = self.adminSGSClient.volumes.get(csd_volume_id)
                LOG.info(_LI('csd volume info: %s'), csd_volume)
                replicate_status = csd_volume.replicate_status
                if (replicate_status
                        not in REPLICATE_STATUS_ACTION_MAPPING.keys()):
                    volume.update({'replicate_status': replicate_status})
                    if (replicate_status == fields.ReplicateStatus.DISABLED
                            and action == 'reverse'):
                        if volume.replicate_mode == constants.REP_MASTER:
                            volume.update(
                                {'replicate_mode': constants.REP_SLAVE,
                                 'access_mode': constants.ACCESS_RO})
                        else:
                            volume.update(
                                {'replicate_mode': constants.REP_MASTER,
                                 'access_mode': constants.ACCESS_RW})
                    volume.save()
                    self.sync_replicates.pop(volume_id)
            except exception.CascadedResourceNotFound:
                if action == 'delete':
                    volume.update({
                        'replicate_status': fields.ReplicateStatus.DELETED})
                else:
                    volume.update({
                        'replicate_status': fields.ReplicateStatus.ERROR})
                volume.save()
                self.sync_replicates.pop(volume_id)
            except Exception as exc:
                LOG.error(_LE("Sync replicate %(volume_id)s status failed,"
                              " err: %(err)s."),
                          {"volume_id": volume_id, "err": exc})

    def _sync_replications_status(self):
        for replication_id, item in self.sync_replications.items():
            action = item['action']
            try:
                replication = objects.Replication.get_by_id(
                    self.admin_context, replication_id)
            except exception.ReplicationNotFound:
                self.sync_replications.pop(replication_id)
                continue
            # already in stable status
            if (replication.status
                    not in REPLICATION_STATUS_ACTION_MAPPING.keys()):
                self.sync_replications.pop(replication_id)
                continue

            if not replication.force:
                master_volume = objects.Volume.get_by_id(
                    self.admin_context, replication['master_volume'])
                master_rep_status = master_volume.replicate_status
            else:
                master_rep_status = fields.ReplicateStatus.FAILED_OVER
            LOG.info(_("master volume replicate status: %s"),
                     master_rep_status)
            if master_rep_status in REPLICATE_STATUS_ACTION_MAPPING.keys():
                continue

            slave_volume = objects.Volume.get_by_id(
                self.admin_context, replication['slave_volume'])
            slave_rep_status = slave_volume.replicate_status
            LOG.info(_("slave volume replicate status: %s"), slave_rep_status)
            if slave_rep_status in REPLICATE_STATUS_ACTION_MAPPING.keys():
                continue

            if slave_rep_status == master_rep_status:
                replicate_status = slave_rep_status
                if replicate_status in [None, fields.ReplicateStatus.DELETED]:
                    master_volume.update({'replicate_mode': None,
                                          'replication_id': None,
                                          'peer_volume': None,
                                          'access_mode': None})
                    master_volume.save()
                    slave_volume.update({'replicate_mode': None,
                                         'replication_id': None,
                                         'peer_volume': None,
                                         'access_mode': None})
                    slave_volume.save()
                    replication.destroy()
                else:
                    if (replicate_status == fields.ReplicateStatus.DISABLED
                            and action == 'reverse'):
                        replication.update(
                            {'status': replicate_status,
                             'master_volume': replication.slave_volume,
                             'slave_volume': replication.master_volume})
                    else:
                        replication.update({'status': replicate_status})
                    replication.save()
            else:
                replication.update({'status': fields.ReplicationStatus.ERROR})
                replication.save()
                self.sync_replications.pop(replication_id)

    def _sync_checkpoints_status(self):
        for checkpoint_id, item in self.sync_checkpoints.items():
            try:
                checkpoint = objects.Checkpoint.get_by_id(self.admin_context,
                                                          checkpoint_id)
            except exception.CheckpointNotFound:
                self.sync_checkpoints.pop(checkpoint_id)
                continue
            # already in stable status
            if None in [checkpoint.master_snapshot, checkpoint.slave_snapshot]:
                continue
            if (checkpoint.status
                    not in CHECKPOINT_STATUS_ACTION_MAPPING.keys()):
                self.sync_checkpoints.pop(checkpoint_id)
                continue
            try:
                master_snap = objects.Snapshot.get_by_id(
                    self.admin_context, checkpoint['master_snapshot'])
                master_snap_status = master_snap.status
            except exception.SnapshotNotFound:
                master_snap_status = fields.SnapshotStatus.DELETED
            LOG.info(_("master snap status: %s"), master_snap_status)
            if master_snap_status in SNAPSHOT_STATUS_ACTION_MAPPING.keys():
                continue

            try:
                slave_snap = objects.Snapshot.get_by_id(
                    self.admin_context, checkpoint['slave_snapshot'])
                slave_snap_status = slave_snap.status
            except exception.SnapshotNotFound:
                slave_snap_status = fields.SnapshotStatus.DELETED
            LOG.info(_("slave snap status: %s"), slave_snap_status)
            if slave_snap_status in SNAPSHOT_STATUS_ACTION_MAPPING.keys():
                continue

            if slave_snap_status == master_snap_status:
                if slave_snap_status == fields.SnapshotStatus.DELETED:
                    checkpoint.destroy()
                else:
                    checkpoint.update({'status': slave_snap_status})
                    checkpoint.save()
            else:
                checkpoint.update({'status': fields.CheckpointStatus.ERROR})
                checkpoint.save()
            self.sync_checkpoints.pop(checkpoint_id)

    def _get_cascaded_sgs_client(self, context=None):
        if context is None:
            return sgs.get_admin_client()
        else:
            return sgs.get_project_context_client(context)

    def _get_cascaded_cinder_client(self, context=None):
        if context is None:
            return cinder.get_admin_client()
        else:
            return cinder.get_project_context_client(context)

    def _get_cascaded_nova_client(self, context=None):
        if context is None:
            return nova.get_admin_client()
        else:
            return nova.get_project_context_client(context)

    def _gen_csd_volume_name(self, csg_volume_id):
        return 'volume@%s' % csg_volume_id

    def _get_csd_cinder_volume_id(self, volume_id):
        # get csd_volume_id from cache mapping as first
        if volume_id in self.volumes_mapping_cache.keys():
            return self.volumes_mapping_cache[volume_id]

        csd_volume_name = self._gen_csd_volume_name(volume_id)
        search_opts = {'all_tenants': True,
                       'name': csd_volume_name}
        try:
            vols = self.adminCinderClient.volumes.list(
                search_opts=search_opts, detailed=True)
            if vols:
                csd_volume_id = vols[0]._info['id']
                self.volumes_mapping_cache[volume_id] = csd_volume_id
                return csd_volume_id
            else:
                msg = _LE("get cascaded cinder volume-id of %s failed" %
                          volume_id)
                LOG.info(msg)
                raise exception.CascadedResourceNotFound(reason=msg)
        except Exception as err:
            LOG.info(_LE("get cascaded cinder volume-id of %s err"), volume_id)
            raise err

    def _get_csd_sgs_volume_id(self, volume_id):
        # get csd_volume_id from cache mapping as first
        if volume_id in self.volumes_mapping_cache.keys():
            return self.volumes_mapping_cache[volume_id]

        csd_volume_name = self._gen_csd_volume_name(volume_id)
        search_opts = {'all_tenants': True,
                       'name': csd_volume_name}
        try:
            vols = self.adminSGSClient.volumes.list(
                search_opts=search_opts)
            if vols:
                csd_volume_id = vols[0]._info['id']
                self.volumes_mapping_cache[volume_id] = csd_volume_id
                return csd_volume_id
            else:
                msg = _LE("get cascaded volume id of %s err" % volume_id)
                LOG.info(msg)
                raise exception.CascadedResourceNotFound(reason=msg)
        except Exception as err:
            LOG.info(_LE("get cascaded volume id of %s err"), volume_id)
            raise err

    def enable_sg(self, context, volume_id):
        LOG.info(_LI("Enable-SG for this volume with id %s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)
        try:
            # step 1: get cascaded volume_id
            csd_volume_id = self._get_csd_cinder_volume_id(volume_id)

            # step 2: enable sg in cascaded
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            display_name = self._gen_csd_volume_name(volume_id)
            metadata = {'logicalVolumeId': volume_id}
            csd_sgs_client.volumes.enable(csd_volume_id, name=display_name,
                                          metadata=metadata)
            # step 3: add to sync status map
            self.volumes_mapping_cache[volume_id] = csd_volume_id
            volume.update_metadata({'logicalVolumeId': csd_volume_id})
            self.sync_volumes[volume_id] = {'volume': volume,
                                            'action': 'enable'}
        except Exception as err:
            LOG.info(_LE("call csd sgs to enable volume:%(volume_id)s failed, "
                         "error:%(err)s"),
                     {"volume_id": volume_id, "err": err})
            self._update_volume_error(volume)

    def disable_sg(self, context, volume_id, cascade=False):
        volume = objects.Volume.get_by_id(context, volume_id)
        try:
            # step 1: get cascaded volume_id
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)

            # step 2: disable sg in cascaded
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.volumes.disable(csd_volume_id)
            # step 3: add to sync status map
            self.volumes_mapping_cache[volume_id] = csd_volume_id
            self.sync_volumes[volume_id] = {'volume': volume,
                                            'action': 'disable'}
        except (NotFound, exception.CascadedResourceNotFound):
            volume.destroy()
        except Exception as err:
            LOG.info(_LE("call csd sgs to disable volume:%(volume_id)s "
                         "failed, error:%(err)s"),
                     {"volume_id": volume_id, "err": err})
            self._update_volume_error(volume)

    def delete(self, context, volume_id):
        LOG.info(_LI("Delete sg volume with id %s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)
        try:
            # step 1: get cascaded volume_id
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
            # step 2: delete in cascaded
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.volumes.delete(csd_volume_id)
            # step 3: add to sync status map
            self.volumes_mapping_cache[volume_id] = csd_volume_id
            self.sync_volumes[volume_id] = {'action': 'delete'}
        except (exception.CascadedResourceNotFound, NotFound):
            volume.destroy()
        except Exception as exc:
            LOG.info(_LE("call csd sgs to delete volume:%(volume_id)s failed,"
                         "error:%(err)"),
                     {'volume_id': volume_id, 'err': exc})
            self._update_volume_error(volume)

    def _gen_csd_instance_name(self, instance_id):
        return 'server@%s' % instance_id

    def _get_csd_instance_id(self, instance_id):
        csd_instance_name = self._gen_csd_instance_name(instance_id)
        search_opts = {'all_tenants': True,
                       'name': csd_instance_name}
        try:
            vms = self.adminNovaClient.servers.list(
                search_opts=search_opts, detailed=True)
            if vms:
                csd_instance_id = vms[0]._info['id']
                return csd_instance_id
            else:
                msg = _LE("get cascaded nova instance-id of %s failed" %
                          instance_id)
                LOG.info(msg)
                raise exception.CascadedResourceNotFound(reason=msg)
        except Exception as err:
            LOG.info(_LE("get cascaded nova instance-id of %s err"),
                     instance_id)
            raise err

    def attach_volume(self, context, volume_id, attachment_id):
        LOG.info(_LI("Attach volume with id: %s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)
        attachment = objects.VolumeAttachment.get_by_id(context, attachment_id)
        instance_uuid = attachment.instance_uuid
        instance_host = attachment.instance_host
        try:
            csd_instance_id = self._get_csd_instance_id(instance_uuid)
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.volumes.attach_volume(csd_volume_id,
                                                 csd_instance_id,
                                                 instance_host)
            self.sync_volumes[volume_id] = {'action': 'attach',
                                            'attachment': attachment}
        except Exception:
            attachment.destroy()
            volume.status = volume.previous_status
            volume.save()

    def detach_volume(self, context, volume_id, attachment_id):
        LOG.info(_LI("Detach volume with id: %s"), volume_id)
        volume = objects.Volume.get_by_id(context, volume_id)
        attachment = objects.VolumeAttachment.get_by_id(context, attachment_id)
        instance_uuid = attachment.instance_uuid
        try:
            csd_instance_id = self._get_csd_instance_id(instance_uuid)
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.volumes.detach_volume(csd_volume_id,
                                                 csd_instance_id)
            self.sync_volumes[volume_id] = {'action': 'detach',
                                            'attachment': attachment}
        except Exception:
            attachment.attach_status = (
                fields.VolumeAttachStatus.ERROR_DETACHING)
            attachment.save()
            volume.status = volume.previous_status
            volume.save()

    def _gen_csd_backup_name(self, backup_id):
        return 'backup@%s' % backup_id

    def _get_csd_backup_id(self, backup_id):
        # get csd_backup_id from cache mapping as first
        if backup_id in self.backups_mapping_cache.keys():
            return self.backups_mapping_cache[backup_id]
        csd_backup_name = self._gen_csd_backup_name(backup_id)
        search_opts = {'all_tenants': True,
                       'name': csd_backup_name}
        try:
            backups = self.adminSGSClient.backups.list(
                search_opts=search_opts)
            if backups:
                csd_backup_id = backups[0]._info['id']
                self.backups_mapping_cache[backup_id] = csd_backup_id
                return csd_backup_id
            else:
                msg = _LE("get cascaded sgs backup-id of %s err" % backup_id)
                LOG.info(msg)
                raise exception.CascadedResourceNotFound(reason=msg)
        except Exception as err:
            LOG.info(_LE("get cascaded sgs backup-id of %s err"), backup_id)
            raise err

    def _update_volume_error(self, volume):
        volume.update({'status': fields.VolumeStatus.ERROR})
        volume.save()

    def _update_backup_error(self, backup):
        backup.update({'status': fields.BackupStatus.ERROR})
        backup.save()

    def create_backup(self, context, backup_id):
        # step 1: check status in cascading level
        backup = objects.Backup.get_by_id(context, backup_id)
        volume_id = backup.volume_id
        LOG.info(_LI("Create backup started, backup:%(backup_id)s, volume: "
                     "%(volume_id)s"),
                 {'volume_id': volume_id, 'backup_id': backup_id})

        volume = objects.Volume.get_by_id(context, volume_id)
        previous_status = volume.get('previous_status', None)

        expected_status = 'backing-up'
        actual_status = volume['status']
        if actual_status != expected_status:
            msg = (_('Create backup aborted, expected volume status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._update_backup_error(backup)
            self.db.volume_update(context, volume_id,
                                  {'status': previous_status,
                                   'previous_status': 'error_backing_up'})
            raise exception.InvalidVolume(reason=msg)

        expected_status = fields.BackupStatus.CREATING
        actual_status = backup['status']
        if actual_status != expected_status:
            msg = (_('Create backup aborted, expected backup status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._update_backup_error(backup)
            self.db.volume_update(context, volume_id,
                                  {'status': previous_status,
                                   'previous_status': 'error_backing_up'})
            raise exception.InvalidBackup(reason=msg)

        try:
            # step 2: call create backup to cascaded level
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            display_name = self._gen_csd_backup_name(backup_id)
            csd_backup = csd_sgs_client.backups.create(
                volume_id=csd_volume_id,
                type=backup['type'],
                destination=backup['destination'],
                name=display_name)

            # step 3: add to sync status map
            self.backups_mapping_cache[backup_id] = csd_backup.id
            self.sync_backups[backup_id] = {'action': 'create'}
            self.sync_volumes[volume_id] = {'action': 'backup'}
        except Exception:
            with excutils.save_and_reraise_exception():
                self._update_backup_error(backup)
                self.db.volume_update(context, volume_id,
                                      {'status': previous_status,
                                       'previous_status': 'error_backing_up'})

    def delete_backup(self, context, backup_id):
        # step 1: check status in cascading level
        backup = objects.Backup.get_by_id(context, backup_id)

        expected_status = fields.BackupStatus.DELETING
        actual_status = backup['status']
        if actual_status != expected_status:
            msg = (_('Delete backup aborted, expected backup status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._update_backup_error(backup)
            raise exception.InvalidBackup(reason=msg)

        try:
            # step 2: call delete backup to cascaded level
            csd_backup_id = self._get_csd_backup_id(backup_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.backups.delete(csd_backup_id)

            # step 3: add to sync status
            self.backups_mapping_cache[backup_id] = csd_backup_id
            self.sync_backups[backup_id] = {'action': 'delete'}
        except (NotFound, exception.CascadedResourceNotFound):
            backup.destroy()
        except Exception as exc:
            LOG.info(_LE("call csd sgs to delete backup:%(backup_id)s "
                         "failed, error:%(err)s"),
                     {"backup_id": backup_id, "err": exc})
            with excutils.save_and_reraise_exception():
                self._update_backup_error(backup)

    def restore_backup(self, context, backup_id, volume_id):
        # step 1: check status in cascading level
        backup = objects.Backup.get_by_id(context, backup_id)
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = 'restoring'
        actual_status = backup['status']
        if actual_status != expected_status:
            msg = (_('Restore backup aborted, expected backup status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._update_backup_error(backup)
            self.db.volume_update(context, volume_id,
                                  {'status': fields.VolumeStatus.ERROR})
            raise exception.InvalidVolume(reason=msg)

        try:
            # step 2: call restore backup to cascaded level
            csd_volume_id = self._get_csd_cinder_volume_id(volume_id)
            csd_backup_id = self._get_csd_backup_id(backup_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            volume.update_metadata({'logicalVolumeId': csd_volume_id})
            csd_sgs_client.backups.restore(backup_id=csd_backup_id,
                                           volume_id=csd_volume_id)

            # step 3: add to sync status
            self.backups_mapping_cache[backup_id] = csd_backup_id
            self.volumes_mapping_cache[volume_id] = csd_volume_id
            self.sync_backups[backup_id] = {'action': 'restore'}
            self.sync_volumes[volume_id] = {'action': 'restore'}
        except Exception as err:
            LOG.info(_LE("call csd sgs to restore backup %(backup_id)s failed,"
                         " error:%(err)s"),
                     {"backup_id": backup_id, "err": err})
            with excutils.save_and_reraise_exception():
                self.db.backup_update(
                    context, backup_id,
                    {'status': fields.BackupStatus.AVAILABLE})
                self.db.volume_update(
                    context, volume_id,
                    {'status': fields.VolumeStatus.ERROR_RESTORING})

    def export_record(self, context, backup_id):
        LOG.info(_LI("Export backup record started, backup:%s"), backup_id)
        try:
            csd_backup_id = self._get_csd_backup_id(backup_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            backup_record = csd_sgs_client.backups.export_record(
                backup_id=csd_backup_id)
            return backup_record.to_dict()
        except Exception as err:
            msg = (_("call csd sgs backup to export-record %(backup_id)s "
                     "failed, err:%(err)s"),
                   {"backup_id": backup_id, "err": err})
            LOG.error(msg)
            raise exception.InvalidBackup(reason=msg)

    def import_record(self, context, backup_id, backup_record):
        LOG.info(_LI('Import record started, backup_record: %s.'),
                 backup_record)
        backup = objects.Backup.get_by_id(context, backup_id)
        backup_type = backup_record.get('backup_record', constants.FULL_BACKUP)
        driver_data = jsonutils.dumps(backup_record.get('driver_data'))

        backup.update({'type': backup_type,
                       'driver_data': driver_data})
        backup.save()
        try:
            csd_backup_name = self._gen_csd_backup_name(backup_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_backup = csd_sgs_client.backups.import_record(
                backup_record=backup_record)
            csd_sgs_client.backups.update(backup_id=csd_backup.id,
                                          name=csd_backup_name)
            backup.update({'status': csd_backup.status})
            backup.save()
        except Exception as err:
            msg = (_("call csd sgs backup to import-record failed,"
                     "err:%s"), err)
            LOG.error(msg)
            backup.update({'status': fields.BackupStatus.ERROR})
            backup.save()
            raise exception.InvalidBackup(reason=msg)

    def _update_snapshot_error(self, snapshot):
        if snapshot.checkpoint_id:
            checkpoint = objects.Checkpoint.get_by_id(self.admin_context,
                                                      snapshot.checkpoint_id)
            checkpoint.update({'status': fields.CheckpointStatus.ERROR})
            checkpoint.save()
        snapshot.update({'status': fields.SnapshotStatus.ERROR})
        snapshot.save()

    def _update_replication_error(self, volume, replication):
        volume.update({'replicate_status': fields.ReplicateStatus.ERROR})
        volume.save()
        peer_volume_id = volume['peer_volume']
        peer_volume = objects.Volume.get_by_id(self.admin_context,
                                               peer_volume_id)
        peer_volume.update({'replicate_status': fields.ReplicateStatus.ERROR})
        peer_volume.save()
        replication.update({'status': fields.ReplicationStatus.ERROR})
        replication.save()

    def _gen_csd_snapshot_name(self, snapshot_id):
        return 'snapshot@%s' % snapshot_id

    def _get_csd_snapshot_id(self, snapshot_id):
        # get csd_snapshot_id from cache mapping as first
        if snapshot_id in self.snapshots_mapping_cache.keys():
            return self.snapshots_mapping_cache[snapshot_id]

        csd_snapshot_name = self._gen_csd_snapshot_name(snapshot_id)
        search_opts = {'all_tenants': True,
                       'name': csd_snapshot_name}
        try:
            snapshots = self.adminSGSClient.snapshots.list(
                search_opts=search_opts)
            if snapshots:
                csd_snapshot_id = snapshots[0]._info['id']
                self.snapshots_mapping_cache[snapshot_id] = csd_snapshot_id
                return csd_snapshot_id
            else:
                msg = _LE("get cascaded snapshot-id of %s err" % snapshot_id)
                LOG.info(msg)
                raise exception.CascadedResourceNotFound(reason=msg)
        except Exception as err:
            raise err

    def create_snapshot(self, context, snapshot_id, volume_id):
        # step 1: check status in cascading level
        snapshot = objects.Snapshot.get_by_id(context, snapshot_id)
        objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.SnapshotStatus.CREATING
        actual_status = snapshot['status']
        if actual_status != expected_status:
            msg = (_('Create snapshot aborted, expected snapshot status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._update_snapshot_error(snapshot)
            raise exception.InvalidSnapshot(reason=msg)

        try:
            # step 2: call create snapshot to cascaded level
            checkpoint_id = snapshot['checkpoint_id']
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            display_name = self._gen_csd_snapshot_name(snapshot_id)
            csd_snapshot = csd_sgs_client.snapshots.create(
                volume_id=csd_volume_id,
                name=display_name,
                checkpoint_id=checkpoint_id)
            # step 3: add to sync status
            self.snapshots_mapping_cache[snapshot_id] = csd_snapshot.id
            self.sync_snapshots[snapshot_id] = {'action': 'create'}
            if checkpoint_id:
                self.sync_checkpoints[checkpoint_id] = {'action': 'create'}
        except Exception:
            with excutils.save_and_reraise_exception():
                self._update_snapshot_error(snapshot)

    def delete_snapshot(self, context, snapshot_id):
        # step 1: check status in cascading level
        snapshot = objects.Snapshot.get_by_id(context, snapshot_id)
        checkpoint_id = snapshot['checkpoint_id']

        expected_status = fields.SnapshotStatus.DELETING
        actual_status = snapshot['status']
        if actual_status != expected_status:
            msg = (_('Delete snapshot aborted, expected backup status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._update_snapshot_error(snapshot)
            raise exception.InvalidBackup(reason=msg)

        try:
            # step 2: call delete snapshot to cascaded level
            csd_snapshot_id = self._get_csd_snapshot_id(snapshot_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.snapshots.delete(snapshot_id=csd_snapshot_id)

            # step 3: add to sync status
            self.snapshots_mapping_cache[snapshot_id] = csd_snapshot_id
            self.sync_snapshots[snapshot_id] = {'action': 'delete'}
            if checkpoint_id:
                self.sync_checkpoints[checkpoint_id] = {'action': 'create'}
        except (NotFound, exception.CascadedResourceNotFound):
            snapshot.destroy()
        except Exception as exc:
            LOG.info(_LE("call csd sgs to delete snapshot:%(snapshot_id)s "
                         "failed, error:%(err)s"),
                     {"snapshot_id": snapshot_id, "err": exc})
            with excutils.save_and_reraise_exception():
                self._update_snapshot_error(snapshot)

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
            self._update_volume_error(volume)
            raise exception.InvalidVolume(reason=msg)

        try:
            # step 2: call rollback snapshot to cascaded level
            csd_snapshot_id = self._get_csd_snapshot_id(snapshot_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.snapshots.rollback(snapshot_id=csd_snapshot_id)

            # step 3: add to sync status
            self.snapshots_mapping_cache[snapshot_id] = csd_snapshot_id
            self.sync_snapshots[snapshot_id] = {'action': 'rollback'}
            self.sync_volumes[volume_id] = {'action': 'rollback'}
            checkpoint_id = snapshot['checkpoint_id']
            if checkpoint_id:
                self.sync_checkpoints[checkpoint_id] = {'action': 'rollback'}
        except Exception:
            with excutils.save_and_reraise_exception():
                self._update_snapshot_error(snapshot)

    def create_replicate(self, context, volume_id):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.ReplicateStatus.ENABLING
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Create replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self.db.volume_update(
                context, volume_id,
                {'replicate_status': fields.ReplicateStatus.ERROR})
            raise exception.InvalidVolume(reason=msg)

        replication_id = volume['replication_id']
        replication = objects.Replication.get_by_id(context,
                                                    replication_id)
        try:
            # step 2: call create replicate to cascaded level
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            mode = volume['replicate_mode']
            peer_volume_id = volume['peer_volume']
            peer_volume = objects.Volume.get_by_id(context, peer_volume_id)
            csd_peer_volume_id = peer_volume.metadata['logicalVolumeId']
            csd_sgs_client.replicates.create(volume_id=csd_volume_id,
                                             mode=mode,
                                             peer_volume=csd_peer_volume_id,
                                             replication_id=replication_id)
            # step 3: add to sync status map
            self.volumes_mapping_cache[volume_id] = csd_volume_id
            self.sync_replicates[volume_id] = {'action': 'create'}
            self.sync_replications[replication_id] = {'action': 'create'}
        except Exception:
            with excutils.save_and_reraise_exception():
                self._update_replication_error(volume, replication)

    def delete_replicate(self, context, volume_id):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)

        replication_id = volume['replication_id']
        replication = objects.Replication.get_by_id(context, replication_id)
        expected_status = fields.ReplicateStatus.DELETING
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Delete replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self.db.volume_update(
                context, volume_id,
                {'replicate_status': fields.ReplicateStatus.ERROR})
            raise exception.InvalidVolume(reason=msg)

        try:
            # step 2: call delete replicate to cascaded level
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.replicates.delete(csd_volume_id)
            # step 3: add to sync status map
            self.sync_replicates[volume_id] = {'action': 'delete'}
            self.sync_replications[replication_id] = {'action': 'delete'}
        except (exception.CascadedResourceNotFound, NotFound):
            volume.update({'replicate_status': fields.ReplicateStatus.DELETED})
            volume.save()
        except Exception as exc:
            LOG.info(_LE("call csd sgs volume to delete replicate:"
                         "%(volume_id)s failed, error:%(err)s"),
                     {"volume_id": volume_id, "err": exc})
            with excutils.save_and_reraise_exception():
                self._update_replication_error(volume, replication)

    def enable_replicate(self, context, volume_id):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)
        replication_id = volume['replication_id']
        replication = objects.Replication.get_by_id(context, replication_id)

        expected_status = fields.ReplicateStatus.ENABLING
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Enable replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self.db.volume_update(
                context, volume_id,
                {'replicate_status': fields.ReplicateStatus.ERROR})
            raise exception.InvalidVolume(reason=msg)

        try:
            # step 2: call enable replicate to cascaded level
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.replicates.enable(csd_volume_id)

            # step 3: add to sync status map
            self.sync_replicates[volume_id] = {'action': 'enable'}
            self.sync_replications[replication_id] = {'action': 'enable'}
        except Exception:
            with excutils.save_and_reraise_exception():
                self._update_replication_error(volume, replication)

    def disable_replicate(self, context, volume_id):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)
        replication_id = volume['replication_id']
        replication = objects.Replication.get_by_id(context, replication_id)

        expected_status = fields.ReplicateStatus.DISABLING
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Disable replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self.db.volume_update(
                context, volume_id,
                {'replicate_status': fields.ReplicateStatus.ERROR})
            raise exception.InvalidVolume(reason=msg)

        try:
            # step 2: call disable replicate to cascaded level
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.replicates.disable(csd_volume_id)

            # step 3: add to sync status
            self.sync_replicates[volume_id] = {'action': 'disable'}
            self.sync_replications[replication_id] = {'action': 'disable'}
        except Exception:
            with excutils.save_and_reraise_exception():
                self._update_replication_error(volume, replication)

    def failover_replicate(self, context, volume_id, checkpoint_id=None,
                           snapshot_id=None, force=False):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)
        replication_id = volume['replication_id']
        replication = objects.Replication.get_by_id(context, replication_id)
        expected_status = fields.ReplicateStatus.FAILING_OVER
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Failover replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s'),
                   {'expected_status': expected_status,
                    'actual_status': actual_status})
            volume.update({'replicate_status': fields.ReplicateStatus.ERROR})
            volume.save()
            raise exception.InvalidVolume(reason=msg)

        try:
            # step 2: call failover replicate to cascaded level
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            replicate_info = csd_sgs_client.replicates.failover(
                csd_volume_id, checkpoint_id, force)
            if snapshot_id:
                csd_snapshot_id = replicate_info.snapshot_id
                self.snapshots_mapping_cache[snapshot_id] = csd_snapshot_id

                csd_snapshot_name = self._gen_csd_snapshot_name(snapshot_id)
                csd_sgs_client.snapshots.update(csd_snapshot_id,
                                                name=csd_snapshot_name)
                self.sync_snapshots[snapshot_id] = {'action': 'create'}
                self.sync_checkpoints[checkpoint_id] = {'action': 'create'}

            # step 3: add to sync status
            self.sync_replicates[volume_id] = {'action': 'failover'}
            self.sync_replications[replication_id] = {'action': 'failover'}
        except Exception:
            with excutils.save_and_reraise_exception():
                if snapshot_id:
                    snapshot = objects.Snapshot.get_by_id(context, snapshot_id)
                    self._update_snapshot_error(snapshot)
                self._update_replication_error(volume, replication)

    def reverse_replicate(self, context, volume_id):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)
        replication_id = volume['replication_id']
        replication = objects.Replication.get_by_id(context, replication_id)
        expected_status = fields.ReplicateStatus.REVERSING
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Reverse replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._update_replication_error(volume, replication)
            raise exception.InvalidVolume(reason=msg)

        try:
            # step 2: call reverse replicate to cascaded level
            csd_volume_id = self._get_csd_sgs_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.replicates.reverse(csd_volume_id)

            # step 3: add to sync status
            self.sync_replicates[volume_id] = {'action': 'reverse'}
            self.sync_replications[replication_id] = {'action': 'reverse'}
        except Exception:
            with excutils.save_and_reraise_exception():
                self._update_replication_error(volume, replication)

    def create_volume(self, context, snapshot_id, volume_id):
        volume = objects.Volume.get_by_id(context, volume_id)
        try:
            csd_snapshot_id = self._get_csd_snapshot_id(snapshot_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_volume_id = self._get_csd_cinder_volume_id(volume_id)
            csd_sgs_client.volumes.create(
                snapshot_id=csd_snapshot_id, volume_id=csd_volume_id)

            self.sync_volumes[volume_id] = {
                'volume': volume,
                'action': 'create'}
        except Exception as err:
            LOG.info(_LE("call csd sgs to create volume from "
                         "snapshot:%(snapshot_id)s failed, error:%(err)s"),
                     {"snapshot_id": snapshot_id, "err": err})
            with excutils.save_and_reraise_exception():
                msg = _("Create volume from snapshot failed")
                LOG.error(msg)
                volume.update({'status': fields.VolumeStatus.ERROR})
                volume.save()
