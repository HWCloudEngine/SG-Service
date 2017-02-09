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

"""Handles all requests relating to controller service."""

import six

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils

from sgservice.controller.client_factory import ClientFactory
from sgservice.controller import rpcapi as protection_rpcapi
from sgservice.db import base
from sgservice import exception
from sgservice.i18n import _, _LI, _LE
from sgservice import objects
from sgservice.objects import fields
from sgservice import utils

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


class API(base.Base):
    """API for interacting with the controller manager."""

    def __init__(self, db_driver=None):
        self.controller_rpcapi = protection_rpcapi.ControllerAPI()
        super(API, self).__init__(db_driver)

    def get(self, context, volume_id):
        try:
            volume = objects.Volume.get_by_id(context, volume_id)
            LOG.info(_LI("Volume info retrieved successfully."),
                     resource=volume)
            return volume
        except Exception:
            raise exception.VolumeNotFound(volume_id)

    def enable_sg(self, context, volume_id, name=None, description=None):
        try:
            self.get(context, volume_id)
            msg = (_LE("The volume '%s' already enabled SG.") % volume_id)
            LOG.error(msg)
            raise exception.InvalidVolume(reason=msg)
        except exception.VolumeNotFound:
            pass

        cinder_client = ClientFactory.create_client("cinder", context)
        try:
            cinder_volume = cinder_client.volumes.get(volume_id)
        except Exception:
            msg = (_("Get the volume '%s' from cinder failed.") % volume_id)
            LOG.error(msg)
            raise exception.InvalidVolume(reason=msg)

        if cinder_volume.status != 'available':
            msg = (_("The cinder volume '%(vol_id)s' status to be enabled sg "
                     "must available, but current is %(status)s.") %
                   {'vol_id': volume_id,
                    'status': cinder_volume.status})
            LOG.error(msg)
            raise exception.InvalidVolume(reason=msg)

        if name is None:
            name = cinder_volume.name
        if description is None:
            description = cinder_volume.description

        volume_properties = {
            'user_id': context.user_id,
            'project_id': context.project_id,
            'status': fields.VolumeStatus.ENABLING,
            'display_name': name,
            'display_description': description,
            'availability_zone': cinder_volume.availability_zone,
            'size': cinder_volume.size
        }

        try:
            objects.Volume.get_by_id(context, id=volume_id,
                                     read_deleted='only')
            volume = objects.Volume.reenable(context, volume_id,
                                             volume_properties)
        except Exception:
            volume_properties['id'] = volume_id
            volume = objects.Volume(context=context, **volume_properties)
            volume.create()
        volume.save()

        self.controller_rpcapi.enable_sg(context, volume=volume)
        return volume

    def disable_sg(self, context, volume, cascade=False):
        if volume.status != fields.VolumeStatus.ENABLED:
            msg = (_('Volume %(vol_id)s to be disabled-sg status must be'
                     'enabled, but current status is %(status)s.')
                   % {'vol_id': volume.id,
                      'status': volume.status})
            raise exception.InvalidVolume(reason=msg)

        if volume.replication_id is not None:
            msg = (_("Volume '%(vol_id)s' belongs to the replication "
                     "'%(rep_id)s', can't be disable.")
                   % {'vol_id': volume.id,
                      'rep_id': volume.replication_id})
            raise exception.InvalidVolume(reason=msg)

        snapshots = objects.SnapshotList.get_all_by_volume(context, volume.id)

        if cascade is False:
            if len(snapshots) != 0:
                msg = _(
                    'Unable disable-sg this volume with snapshot or backup.')
                raise exception.InvalidVolume(reason=msg)

        excepted = {'status': ('available', 'error', 'deleting')}
        values = {'status': 'deleting'}
        if cascade:
            for s in snapshots:
                result = s.conditional_update(values, excepted)
                if not result:
                    volume.update({'status': fields.VolumeStatus.ERROR})
                    volume.save()

                    msg = _('Failed to update snapshot.')
                    raise exception.InvalidVolume(reason=msg)

        volume.update({'status': 'disabling'})
        volume.save()
        self.controller_rpcapi.disable_sg(context, volume=volume,
                                          cascade=cascade)
        return volume

    def get_all(self, context, marker=None, limit=None, sort_keys=None,
                sort_dirs=None, filters=None, offset=None):
        if filters is None:
            filters = {}

        all_tenants = utils.get_bool_params('all_tenants', filters)

        try:
            if limit is not None:
                limit = int(limit)
                if limit < 0:
                    msg = _('limit param must be positive')
                    raise exception.InvalidInput(reason=msg)
        except ValueError:
            msg = _('limit param must be an integer')
            raise exception.InvalidInput(reason=msg)

        if filters:
            LOG.debug("Searching by: %s.", six.text_type(filters))

        if context.is_admin and all_tenants:
            # Need to remove all_tenants to pass the filtering below.
            del filters['all_tenants']
            volumes = objects.VolumeList.get_all(context, marker, limit,
                                                 sort_keys=sort_keys,
                                                 sort_dirs=sort_dirs,
                                                 filters=filters,
                                                 offset=offset)
        else:
            volumes = objects.VolumeList.get_all_by_project(
                context, context.project_id, marker, limit,
                sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
                offset=offset)

        LOG.info(_LI("Get all volumes completed successfully."))
        return volumes

    def reserve_volume(self, context, volume):
        if volume.status != fields.VolumeStatus.ENABLED:
            msg = (_('Volume %(vol_id)s to be reserved status must be'
                     'enabled, but current status is %(status)s.')
                   % {'vol_id': volume.id,
                      'status': volume.status})
            raise exception.InvalidVolume(reason=msg)
        volume.update({'status': 'attaching'})
        volume.save()
        LOG.info(_LI("Reserve volume completed successfully."))

    def unreserve_volume(self, context, volume):
        expected = {'status': 'attaching'}
        value = {'status': 'enabled'}
        result = volume.conditional_update(value, expected)
        if not result:
            msg = (_('Unable to unreserve volume. Volume %(vol_id)s to be '
                     'unreserved status must be attaching, but current '
                     'status is %(status)s.')
                   % {'vol_id': volume.id,
                      'status': volume.status})
            raise exception.InvalidVolume(reason=msg)
        LOG.info(_LI("Unreserve volume completed successfully."))

    def begin_detaching(self, context, volume):
        expected = {'status': 'in-use'}
        value = {'status': 'detaching'}
        result = volume.conditional_update(value, expected)

        if not result:
            msg = (_('Unable to detach volume. Volume %(vol_id)s to be '
                     'unreserved  status  must be in-use, but current '
                     'status is %(status)s.')
                   % {'vol_id': volume.id,
                      'status': volume.status})
            raise exception.InvalidVolume(reason=msg)
        LOG.info(_LI("Begin detaching volume completed successfully."))

    def roll_detaching(self, context, volume):
        expected = {'status': 'detaching'}
        value = {'status': 'in-use'}
        volume.conditional_update(value, expected)
        LOG.info(_LI("Roll detaching of volume completed successfully."))

    def attach(self, context, volume, instance_uuid, host_name, mountpoint,
               mode):
        attach_results = self.controller_rpcapi.attach_volume(context,
                                                              volume,
                                                              instance_uuid,
                                                              host_name,
                                                              mountpoint,
                                                              mode)
        LOG.info(_LI("Attach volume completed successfully."))
        return attach_results

    def detach(self, context, volume, attachment_id):
        detach_results = self.controller_rpcapi.detach_volume(context,
                                                              volume,
                                                              attachment_id)
        LOG.info(_LI("Detach volume completed successfully."))
        return detach_results

    def initialize_connection(self, context, volume, connector):
        init_results = self.controller_rpcapi.initialize_connection(context,
                                                                    volume,
                                                                    connector)
        LOG.info(_LI("Initialize volume connection completed successfully."))
        return init_results

    def get_backup(self, context, backup_id):
        try:
            backup = objects.Backup.get_by_id(context, backup_id)
            LOG.info(_LI("Backup info retrieved successfully."),
                     resource=backup)
            return backup
        except Exception:
            raise exception.BackupNotFound(backup_id)

    def create_backup(self, context, name, description, volume,
                      backup_type='incremental', backup_destination='local'):
        if volume['status'] not in ['enabled', 'in-use']:
            msg = (_('Volume to be backed up should be enabled or in-use, '
                     'but current status is "%s".') % volume['status'])
            raise exception.InvalidVolume(reason=msg)

        previous_status = volume['status']
        self.db.volume_update(context, volume['id'],
                              {'status': 'backing-up',
                               'previous_status': previous_status})
        backup = None
        try:
            kwargs = {
                'use_id': context.user_id,
                'project_id': context.project_id,
                'display_name': name,
                'display_description': description,
                'volume_id': volume['id'],
                'status': fields.BackupStatus.CREATING,
                'size': volume['size'],
                'type': backup_type,
                'destination': backup_destination,
                'availability_zone': volume['availability_zone'],
                'replication_zone': volume['replication_zone']
            }
            backup = objects.Backup(context, **kwargs)
            backup.create()
            backup.save()
        except Exception:
            with excutils.save_and_reraise_exception():
                if backup and 'id' in backup:
                    backup.destroy()
                self.db.volume_update(context, volume['id'],
                                      {'status': previous_status})

        self.controller_rpcapi.create_backup(context, backup)
        return backup

    def delete_backup(self, context, backup):
        backup.update({'status': fields.BackupStatus.DELETING})
        backup.save()
        self.controller_rpcapi.delete_backup(context, backup)

    def restore_backup(self, context, backup, volume):
        if volume['status'] != fields.VolumeStatus.ENABLED:
            msg = _('Volume to be restored must be enabled')
            raise exception.InvalidVolume(reason=msg)

        if backup['size'] > volume['size']:
            msg = (_('Volume size %(volume_size)d is too small to restore '
                     'backup of size %(size)d') %
                   {'volume_size': volume['size'], 'size': backup['size']})
            raise exception.InvalidVolume(reason=msg)

        backup_type = backup['type']
        if backup_type == 'local':
            if backup['availability_zone'] != volume['availability_zone']:
                msg = _('Local backup and volume are not in the same zone')
                raise exception.InvalidVolume(reason=msg)
        else:
            if backup['replication_zone'] != volume['availability_zone']:
                msg = _('Remote backup and volume are not in the same zone')
                raise exception.InvalidVolume(reason=msg)

        backup.update({'status': fields.BackupStatus.RESTORING})
        backup.save()

        volume.update({'status': fields.VolumeStatus.RESTORING_BACKUP})
        volume.save()

        self.controller_rpcapi.restore_backup(backup, volume)

        restore = {
            'backup_id': backup['id'],
            'volume_id': volume['id'],
            'volume_name': volume['display_name']
        }

        return restore

    def get_all_backups(self, context, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None):
        if filters is None:
            filters = {}

        all_tenants = utils.get_bool_params('all_tenants', filters)

        try:
            if limit is not None:
                limit = int(limit)
                if limit < 0:
                    msg = _('limit param must be positive')
                    raise exception.InvalidInput(reason=msg)
        except ValueError:
            msg = _('limit param must be an integer')
            raise exception.InvalidInput(reason=msg)

        if filters:
            LOG.debug("Searching by: %s.", six.text_type(filters))

        if context.is_admin and all_tenants:
            # Need to remove all_tenants to pass the filtering below.
            del filters['all_tenants']
            backups = objects.BackupList.get_all(context, marker, limit,
                                                 sort_keys=sort_keys,
                                                 sort_dirs=sort_dirs,
                                                 filters=filters,
                                                 offset=offset)
        else:
            backups = objects.BackupList.get_all_by_project(
                context, context.project_id, marker, limit,
                sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
                offset=offset)

        LOG.info(_LI("Get all backups completed successfully."))
        return backups

    def get_snapshot(self, context, snapshot_id):
        try:
            snapshot = objects.Snapshot.get_by_id(context, snapshot_id)
            LOG.info(_LI("Snapshot info retrieved successfully."),
                     resource=snapshot)
            return snapshot
        except Exception:
            raise exception.SnapshotNotFound(snapshot_id)

    def create_snapshot(self, context, name, description, volume,
                        checkpoint_id=None):
        if volume['status'] not in ['enabled', 'in-use']:
            msg = (_('Volume to create snapshot should be enabled or in-use, '
                     'but current status is "%s".') % volume['status'])
            raise exception.InvalidVolume(reason=msg)

        snapshot = None
        try:
            destination = 'remote' if checkpoint_id else 'local'
            kwargs = {
                'use_id': context.user_id,
                'project_id': context.project_id,
                'display_name': name,
                'display_description': description,
                'volume_id': volume['id'],
                'status': fields.SnapshotStatus.CREATING,
                'checkpoint_id': checkpoint_id,
                'destination': destination,
                'availability_zone': volume['availability_zone'],
                'replication_zone': volume['replication_zone']
            }
            snapshot = objects.Snapshot(context, **kwargs)
            snapshot.create()
            snapshot.save()
        except Exception:
            with excutils.save_and_reraise_exception():
                if snapshot and 'id' in snapshot:
                    snapshot.destroy()

        self.controller_rpcapi.create_snapshot(context, snapshot)
        return snapshot

    def delete_snapshot(self, context, snapshot):
        if snapshot['status'] not in [fields.SnapshotStatus.AVAILABLE,
                                      fields.SnapshotStatus.ERROR]:
            msg = _('Snapshot to be deleted must be available or error')
            raise exception.InvalidSnapshot(reason=msg)

        self.db.snapshot_update(context, snapshot['id'],
                                {'status': fields.SnapshotStatus.DELETING})
        self.controller_rpcapi.delete_snapshot(context, snapshot)

    def get_all_snapshots(self, context, marker=None, limit=None,
                          sort_keys=None, sort_dirs=None, filters=None,
                          offset=None):
        if filters is None:
            filters = {}

        all_tenants = utils.get_bool_params('all_tenants', filters)

        try:
            if limit is not None:
                limit = int(limit)
                if limit < 0:
                    msg = _('limit param must be positive')
                    raise exception.InvalidInput(reason=msg)
        except ValueError:
            msg = _('limit param must be an integer')
            raise exception.InvalidInput(reason=msg)

        if filters:
            LOG.debug("Searching by: %s.", six.text_type(filters))

        if context.is_admin and all_tenants:
            # Need to remove all_tenants to pass the filtering below.
            del filters['all_tenants']
            snapshots = objects.SnapshotList.get_all(context, marker, limit,
                                                     sort_keys=sort_keys,
                                                     sort_dirs=sort_dirs,
                                                     filters=filters,
                                                     offset=offset)
        else:
            snapshots = objects.SnapshotList.get_all_by_project(
                context, context.project_id, marker, limit,
                sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
                offset=offset)

        LOG.info(_LI("Get all snapshots completed successfully."))
        return snapshots
