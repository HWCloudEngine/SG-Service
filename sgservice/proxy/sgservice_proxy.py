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

from sgservice.i18n import _, _LI
from sgservice.controller.api import API as ServiceAPI
from sgservice import exception
from sgservice import objects
from sgservice.objects import fields
from sgservice import manager
from sgservice import utils

proxy_manager_opts = [
    # cfg.IntOpt('sync_interval',
    #            default=5,
    #            help='seconds between cascading and cascaded sgservices when '
    #                 'synchronizing data'),
    # cfg.IntOpt('status_query_count',
    #            default=5,
    #            help='status query times'),
    # cfg.StrOpt('sgservice_username',
    #            default='sgservice_username',
    #            help='username for connecting to cinder in admin context'),
    # cfg.StrOpt('admin_password',
    #            default='admin_password',
    #            help='password for connecting to cinder in admin context'),
    # cfg.StrOpt('sgservice_tenant_name',
    #            default='sgservice_tenant_name',
    #            help='tenant name for connecting to cinder in admin context'),
    # cfg.StrOpt('sgservice_tenant_id',
    #            default='sgservice_tenant_id',
    #            help='tenant id for connecting to cinder in admin context'),
    # cfg.StrOpt('cascaded_availability_zone',
    #            default='nova',
    #            help='availability zone for cascaded OpenStack'),
    # cfg.StrOpt('keystone_auth_url',
    #            default='http://127.0.0.1:5000/v2.0/',
    #            help='value of keystone url'),
    # cfg.StrOpt('cascading_sgservice_url',
    #            default='http://127.0.0.1:8975/v1/%(project_id)s',
    #            help='value of cascading sgservice url'),
    # cfg.StrOpt('casacaded_cinder_url',
    #            default='http://127.0.0.1:8776/v2/%(project_id)s',
    #            help='value of cascaded cinder url'),
    # cfg.StrOpt('cascaded_region_name',
    #            default='RegionOne',
    #            help='Region name of this node')
]

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

        self.volumes_mapping = {}
        self.backups_mapping = {}
        self.snapshots_mapping = {}
        self.replicates_mapping = {}

    def init_host(self, **kwargs):
        """ list all ing-status objects(volumes, snapshot, backups);
            add to self.volumes_mapping, backups_mapping, snapshots_mapping,
            and replicates_mapping;
            start looping call to sync volumes, backups, snapshots,
            and replicates status;
        """
        # TODO(luobin)
        pass

    def _sync_volumes_status(self):
        """ sync cascaded volumes'(in volumes_mapping) status;
            and update cascading volumes' status
        """
        # TODO(luobin)
        pass

    def _sync_backups_status(self):
        """ sync cascaded backups'(in volumes_mapping) status;
            update cascading backups' status
        """
        # TODO(luobin)
        pass

    def _sync_snapshots_status(self):
        """ sync cascaded snapshots'(in volumes_mapping) status;
            update cascading snapshots' status;
            update cascading checkpoints' status if needed;
        """
        # TODO(luobin)
        pass

    def _sync_replicates_status(self):
        """ sync cascaded volumes'(in volumes_mapping) replicate-status;
            update cascading volumes' replicate-status;
            update cascading replications' status;
        """
        # TODO(luobin)
        pass

    def _get_cascaded_sgs_client(self, context=None):
        # TODO(luobin): get cascaded sgs client
        if context is None:
            # use use_name and password to get admin sgs client
            return
        else:
            return

    def _get_cascaded_cinder_client(self, context=None):
        # TODO(luobin): get cascaded cinder client
        if context is None:
            # use use_name and password to get admin cinder client
            return
        else:
            return

    def _gen_csd_volume_name(self, csg_volume_id):
        return 'volume@%s' % csg_volume_id

    def _get_csd_volume_id(self, volume_id):
        # TODO(luobin): get csd_volume_id from cache mapping as first
        csd_volume_name = self._gen_csd_volume_name(volume_id)
        search_opts = {'all_tenants': True,
                       'name': csd_volume_name}
        try:
            vols = self.adminCinderClient.volumes.list(
                search_opts=search_opts, detailed=True)
            if vols:
                return vols[0]._info['id']
        except Exception as err:
            raise err

    def enable_sg(self, context, volume_id):
        # step 1: get cascaded volume_id
        csd_volume_id = self._get_csd_volume_id(volume_id)

        # step 2: enable sg in cascaded
        csd_sgs_client = self._get_cascaded_sgs_client(context)
        csd_sgs_client.volumes.enable_sg(csd_volume_id)

        # step 3: add to sync status map
        self.volumes_mapping[volume_id] = csd_volume_id

    def disable_sg(self, context, volume_id):
        # step 1: get cascaded volume_id
        csd_volume_id = self._get_csd_volume_id(volume_id)

        # step 2: enable sg in cascaded
        csd_sgs_client = self._get_cascaded_sgs_client(context)
        csd_sgs_client.volumes.disable_sg(csd_volume_id)

        # step 3: add to sync status map
        self.volumes_mapping[volume_id] = csd_volume_id

    def attach_volume(self, context, volume_id, instance_uuid, host_name,
                      mountpoint, mode):
        """cascaded attach_volume will be called in nova-proxy
           sgservice-proxy just update cascading level data
        """
        volume = objects.Volume.get_by_id(context, volume_id)
        if volume['status'] == 'attaching':
            access_mode = volume['access_mode']
            if access_mode is not None and access_mode != mode:
                raise exception.InvalidVolume(
                    reason=_('being attached by different mode'))

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

        self.db.volume_attached(context.elevated(),
                                attachment_id,
                                instance_uuid,
                                host_name_sanitized,
                                mountpoint,
                                mode)
        LOG.info(_LI("Attach volume completed successfully."))

    def initialize_connection(self, context, volume_id, connector):
        # just need return None
        return None

    def _gen_csd_backup_name(self, backup_id):
        return 'backup@%s' % backup_id

    def _get_csd_backup_id(self, backup_id):
        # TODO(luobin): get csd_backup_id from cache mapping as first
        csd_backup_name = self._gen_csd_backup_name(backup_id)
        search_opts = {'all_tenants': True,
                       'name': csd_backup_name}
        try:
            backups = self.adminSGSClient.backups.list(
                search_opts=search_opts)
            if backups:
                return backups[0]._info['id']
        except Exception as err:
            raise err

    def _update_backup_error(self, backup):
        backup.update({'status': fields.BackupStatus.ERROR})
        backup.save()

    def create_backup(self, context, backup_id):
        # step 1: check status in cascading level
        backup = objects.Backup.get_by_id(context, backup_id)
        volume_id = backup['volume_id']
        backup = objects.Backup.get_by_id(context, backup_id)
        volume_id = backup.volume_id
        LOG.info(_LI("Create backup started, backup:%(backup_id)s, volume: "
                     "%(volume_id)s"),
                 {'volume_id': volume_id, 'backup_id': backup_id})

        volume = objects.Volume.get_by_id(context, volume_id)
        previous_status = volume.get('previous-status', None)

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
            csd_volume_id = self._get_csd_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            display_name = "backup@%s" % backup_id
            csd_backup = csd_sgs_client.backups.create(
                volume_id=csd_volume_id,
                type=backup['type'],
                destination=backup['destination'],
                name=display_name)

            # step 3: add to sync status map
            self.backups_mapping[backup_id] = csd_backup['id']
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
            self.backups_mapping[backup_id] = csd_backup_id
        except Exception:
            with excutils.save_and_reraise_exception():
                self._update_backup_error(backup)

    def restore_backup(self, context, backup_id, volume_id):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)
        backup = objects.Backup.get_by_id(context, backup_id)

        expected_status = 'restoring-backup'
        actual_status = volume['status']
        if actual_status != expected_status:
            msg = (_('Restore backup aborted, expected volume status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self.db.backup_update(
                context, backup_id,
                {'status': fields.BackupStatus.AVAILABLE})
            self.db.volume_update(
                context, volume_id,
                {'status': fields.VolumeStatus.ERROR_RESTORING})
            raise exception.InvalidVolume(reason=msg)

        expected_status = 'restoring'
        actual_status = volume['status']
        if actual_status != expected_status:
            msg = (_('Restore backup aborted, expected volume status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self._update_backup_error(backup)
            self.db.volume_update(context, volume_id,
                                  {'status': fields.VolumeStatus.ERROR})
            raise exception.InvalidVolume(reason=msg)

        try:
            # step 2: call restore backup to cascaded level
            csd_volume_id = self._get_csd_volume_id(volume_id)
            csd_backup_id = self._get_csd_backup_id(backup_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.backups.restore(backup_id=csd_backup_id,
                                           volume_id=csd_volume_id)

            # step 3: add to sync status
            self.backups_mapping[backup_id] = csd_backup_id
            self.volumes_mapping[volume_id] = csd_volume_id
        except Exception:
            with excutils.save_and_reraise_exception():
                self.db.backup_update(
                    context, backup_id,
                    {'status': fields.BackupStatus.AVAILABLE})
                self.db.volume_update(
                    context, volume_id,
                    {'status': fields.VolumeStatus.ERROR_RESTORING})

    def _update_snapshot_error(self, snapshot):
        snapshot.update({'status': fields.SnapshotStatus.ERROR})
        snapshot.save()

    def _gen_csd_snapshot_name(self, snapshot_id):
        return 'snapshot@%s' % snapshot_id

    def _get_csd_snapshot_id(self, snapshot_id):
        # TODO(luobin): get csd_snapshot_id from cache mapping as first
        csd_snapshot_name = self._gen_csd_snapshot_name(snapshot_id)
        search_opts = {'all_tenants': True,
                       'name': csd_snapshot_name}
        try:
            snapshots = self.adminSGSClient.snapshots.list(
                search_opts=search_opts)
            if snapshots:
                return snapshots[0]._info['id']
        except Exception as err:
            raise err

    def create_snapshot(self, context, snapshot_id):
        # step 1: check status in cascading level
        snapshot = objects.Snapshot.get_by_id(context, snapshot_id)
        volume_id = snapshot.volume_id
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
            csd_volume_id = self._get_csd_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            display_name = self._gen_csd_snapshot_name(snapshot_id)
            csd_snapshot = csd_sgs_client.snapshots.create(
                volume_id=csd_volume_id,
                name=display_name,
                checkpoint_id=snapshot['checkpoint-id'])

            # step 3: add to sync status
            self.snapshots_mapping[snapshot_id] = csd_snapshot['id']
        except Exception:
            with excutils.save_and_reraise_exception():
                self._update_snapshot_error(snapshot)

    def delete_snapshot(self, context, snapshot_id):
        # step 1: check status in cascading level
        snapshot = objects.Snapshot.get_by_id(context, snapshot_id)

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
            csd_snapshot_id = self._get_csd_volume_id(snapshot_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            csd_sgs_client.snapshots.delete(snapshot_id=csd_snapshot_id)

            # step 3: add to sync status
            self.snapshots_mapping[snapshot_id] = csd_snapshot_id
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

        try:
            # step 2: call create replicate to cascaded level
            csd_volume_id = self._get_csd_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            mode = volume['replicate_mode']
            csd_peer_volume = self._get_csd_volume_id(volume['peer_volume'])
            replication_id = volume['replication_id']
            csd_sgs_client.replicates.create(volume_id=volume_id,
                                             mode=mode,
                                             peer_volume=csd_peer_volume,
                                             replication_id=replication_id)

            # step 3: add to sync status
            self.replicates_mapping[volume_id] = csd_volume_id
        except Exception:
            with excutils.save_and_reraise_exception():
                self.db.volume_update(
                    context, volume_id,
                    {'replicate_status': fields.ReplicateStatus.ERROR})

    def delete_replicate(self, context, volume_id):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)

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
            csd_volume_id = self._get_csd_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            self.driver.enable_replicate(volume=volume)
            csd_sgs_client.replicates.delete(csd_volume_id)

            # step 3: add to sync status
            self.replicates_mapping[volume_id] = csd_volume_id
        except Exception:
            with excutils.save_and_reraise_exception():
                self.db.volume_update(
                    context, volume_id,
                    {'replicate_status': fields.ReplicateStatus.ERROR})

    def enable_replicate(self, context, volume_id):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)

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
            csd_volume_id = self._get_csd_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            self.driver.enable_replicate(volume=volume)
            csd_sgs_client.replicates.enable(csd_volume_id)

            # step 3: add to sync status
            self.replicates_mapping[volume_id] = csd_volume_id
        except Exception:
            with excutils.save_and_reraise_exception():
                self.db.volume_update(
                    context, volume_id,
                    {'replicate_status': fields.ReplicateStatus.ERROR})

    def disable_replicate(self, context, volume_id):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)

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
            csd_volume_id = self._get_csd_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            self.driver.enable_replicate(volume=volume)
            csd_sgs_client.replicates.disable(csd_volume_id)

            # step 3: add to sync status
            self.replicates_mapping[volume_id] = csd_volume_id
        except Exception:
            with excutils.save_and_reraise_exception():
                self.db.volume_update(
                    context, volume_id,
                    {'replicate_status': fields.ReplicateStatus.ERROR})

    def failover_replicate(self, context, volume_id):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.ReplicateStatus.FAILING_OVER
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Failover replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self.db.volume_update(
                context, volume_id,
                {'replicate_status': fields.ReplicateStatus.ERROR})
            raise exception.InvalidVolume(reason=msg)

        try:
            # step 2: call failover replicate to cascaded level
            csd_volume_id = self._get_csd_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            self.driver.enable_replicate(volume=volume)
            csd_sgs_client.replicates.failover(csd_volume_id)

            # step 3: add to sync status
            self.replicates_mapping[volume_id] = csd_volume_id
        except Exception:
            with excutils.save_and_reraise_exception():
                self.db.volume_update(
                    context, volume_id,
                    {'replicate_status': fields.ReplicateStatus.ERROR})

    def reverse_replicate(self, context, volume_id):
        # step 1: check status in cascading level
        volume = objects.Volume.get_by_id(context, volume_id)

        expected_status = fields.ReplicateStatus.REVERSING
        actual_status = volume['replicate_status']
        if actual_status != expected_status:
            msg = (_('Reverse replicate aborted, expected replicate status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            self.db.volume_update(
                context, volume_id,
                {'replicate_status': fields.ReplicateStatus.ERROR})
            raise exception.InvalidVolume(reason=msg)

        try:
            # step 2: call reverse replicate to cascaded level
            csd_volume_id = self._get_csd_volume_id(volume_id)
            csd_sgs_client = self._get_cascaded_sgs_client(context)
            self.driver.enable_replicate(volume=volume)
            csd_sgs_client.replicates.reverse(csd_volume_id)

            # step 3: add to sync status
            self.replicates_mapping[volume_id] = csd_volume_id
        except Exception:
            with excutils.save_and_reraise_exception():
                self.db.volume_update(
                    context, volume_id,
                    {'replicate_status': fields.ReplicateStatus.ERROR})
