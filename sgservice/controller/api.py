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

import random
import six

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils

from sgservice.common import constants
from sgservice import context
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

    def _is_controller_service_enabled(self, availability_zone, host):
        """Check if there is a controller service available."""
        topic = CONF.controller_topic
        ctxt = context.get_admin_context()
        services = objects.ServiceList.get_all_by_topic(
            ctxt, topic, disabled=False)
        for srv in services:
            if (self._az_matched(srv, availability_zone) and
                        srv.host == host and srv.is_up):
                return True
        return False

    def _get_any_available_controller_service(self, availability_zone):
        """Get an available controller service host.

        Get an available controller service host in the specified
        availability zone.
        """
        services = [srv for srv in self._list_controller_services()]
        random.shuffle(services)
        # Get the next running service with matching availability zone.
        idx = 0
        while idx < len(services):
            srv = services[idx]
            if (self._az_matched(srv, availability_zone) and
                    srv.is_up):
                return srv.host
            idx += 1
        return None

    def _get_available_controller_service_host(self, az):
        """Return an appropriate controller service host."""
        controller_host = None
        controller_host = self._get_any_available_controller_service(az)
        if not controller_host:
            raise exception.ServiceNotFound(service_id='sgservice-controller')
        return controller_host

    def _list_controller_services(self):
        """List all enabled controller services.

        :returns: list -- hosts for services that are enabled for sgservice.
        """
        topic = CONF.controller_topic
        ctxt = context.get_admin_context()
        services = objects.ServiceList.get_all_by_topic(
            ctxt, topic, disabled=False)
        return services

    def _list_controller_hosts(self):
        services = self._list_controller_services()
        return [srv.host for srv in services
                if not srv.disabled and srv.is_up]

    def _az_matched(self, service, availability_zone):
        return ((not availability_zone) or
                service.availability_zone == availability_zone)

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

        availability_zone = cinder_volume.availability_zone
        host = self._get_available_controller_service_host(
            az=availability_zone)
        volume_properties = {
            'host': host,
            'user_id': context.user_id,
            'project_id': context.project_id,
            'status': fields.VolumeStatus.ENABLING,
            'display_name': name,
            'display_description': description,
            'availability_zone': availability_zone,
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

        excepted_status = [fields.SnapshotStatus.AVAILABLE,
                           fields.SnapshotStatus.ERROR,
                           fields.SnapshotStatus.DELETING]
        if cascade:
            for s in snapshots:
                if s['status'] not in excepted_status:
                    msg = _('Failed to update snapshot.')
                    raise exception.InvalidVolume(reason=msg)
                else:
                    s.update({'status': fields.SnapshotStatus.DELETING})
                    s.save()

        volume.update({'status': fields.VolumeStatus.DISABLING})
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
            volumes = objects.VolumeList.get_all(
                context, marker=marker, limit=limit, sort_keys=sort_keys,
                sort_dirs=sort_dirs, filters=filters, offset=offset)
        else:
            volumes = objects.VolumeList.get_all_by_project(
                context, project_id=context.project_id, marker=marker,
                limit=limit, sort_keys=sort_keys, sort_dirs=sort_dirs,
                filters=filters, offset=offset)

        LOG.info(_LI("Get all volumes completed successfully."))
        return volumes

    def reserve_volume(self, context, volume):
        if volume.status != fields.VolumeStatus.ENABLED:
            msg = (_('Volume %(vol_id)s to be reserved status must be'
                     'enabled, but current status is %(status)s.')
                   % {'vol_id': volume.id,
                      'status': volume.status})
            raise exception.InvalidVolume(reason=msg)
        volume.update({'status': fields.VolumeStatus.ATTACHING})
        volume.save()
        LOG.info(_LI("Reserve volume completed successfully."))

    def unreserve_volume(self, context, volume):
        expected_status = fields.VolumeStatus.ATTACHING
        actual_status = volume['status']
        if actual_status != expected_status:
            msg = (_('Unreserve volume aborted, expected volume status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            raise exception.InvalidVolume(reason=msg)

        volume.update({'status': fields.VolumeStatus.ENABLED})
        volume.save()
        LOG.info(_LI("Unreserve volume completed successfully."))

    def begin_detaching(self, context, volume):
        expected_status = fields.VolumeStatus.IN_USE
        actual_status = volume['status']
        if actual_status != expected_status:
            msg = (_('Begin detaching volume aborted, expected volume status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            raise exception.InvalidVolume(reason=msg)

        volume.update({'status': fields.VolumeStatus.DETACHING})
        volume.save()
        LOG.info(_LI("Begin detaching volume completed successfully."))

    def roll_detaching(self, context, volume):
        expected_status = fields.VolumeStatus.DETACHING
        actual_status = volume['status']
        if actual_status != expected_status:
            msg = (_('Roll detaching volume aborted, expected volume status '
                     '%(expected_status)% but got %(actual_status)s')
                   % {'expected_status': expected_status,
                      'actual_status': actual_status})
            raise exception.InvalidVolume(reason=msg)

        volume.update({'status': fields.VolumeStatus.IN_USE})
        volume.save()
        LOG.info(_LI("Roll detaching of volume completed successfully."))

    def attach(self, context, volume, instance_uuid, host_name, mountpoint,
               mode):
        access_mode = volume['access_mode']
        if access_mode is not None and access_mode != mode:
            LOG.error(_('being attached by different mode'))
            raise exception.InvalidVolumeAttachMode(mode=mode,
                                                    volume_id=volume.id)

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
        if volume['status'] not in [fields.VolumeStatus.ENABLED,
                                    fields.VolumeStatus.IN_USE]:
            msg = (_('Volume to be backed up should be enabled or in-use, '
                     'but current status is "%s".') % volume['status'])
            raise exception.InvalidVolume(reason=msg)

        previous_status = volume['status']
        volume.update({'status': fields.VolumeStatus.BACKING_UP,
                       'previous_status': previous_status})
        volume.save()
        backup = None
        host = volume['host']
        try:
            kwargs = {
                'host': host,
                'user_id': context.user_id,
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
                volume.update({'status': previous_status})
                volume.save()

        self.controller_rpcapi.create_backup(context, backup)
        return backup

    def delete_backup(self, context, backup):
        backup.update({'status': fields.BackupStatus.DELETING})
        backup.save()
        self.controller_rpcapi.delete_backup(context, backup)

    def restore_backup(self, context, backup, volume_id):
        cinder_client = ClientFactory.create_client("cinder", context)
        try:
            c_volume = cinder_client.volumes.get(volume_id)
        except Exception:
            msg = (_("Get the volume '%s' from cinder failed.") % volume_id)
            LOG.error(msg)
            raise exception.InvalidVolume(reason=msg)

        if c_volume.status != 'available':
            msg = (_("The cinder volume '%(vol_id)s' status to be restore "
                     "backup must available, but current is %(status)s.") %
                   {'vol_id': volume_id,
                    'status': c_volume.status})
            LOG.error(msg)
            raise exception.InvalidVolume(reason=msg)

        backup_type = backup['type']
        if backup_type == 'local':
            if backup['availability_zone'] != c_volume.availability_zone:
                msg = _('Local backup and volume are not in the same zone')
                raise exception.InvalidVolume(reason=msg)
        else:
            if backup['replication_zone'] != c_volume.availability_zone:
                msg = _('Remote backup and volume are not in the same zone')
                raise exception.InvalidVolume(reason=msg)

        backup.update({'status': fields.BackupStatus.RESTORING})
        backup.save()

        self.controller_rpcapi.restore_backup(context, backup, c_volume)

        restore = {
            'backup_id': backup['id'],
            'volume_id': c_volume.id,
            'volume_name': c_volume.name
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
            backups = objects.BackupList.get_all(
                context, marker=marker, limit=limit, sort_keys=sort_keys,
                sort_dirs=sort_dirs, filters=filters, offset=offset)
        else:
            backups = objects.BackupList.get_all_by_project(
                context, project_id=context.project_id, marker=marker,
                limit=limit, sort_keys=sort_keys, sort_dirs=sort_dirs,
                filters=filters, offset=offset)

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
        if volume['status'] not in [fields.VolumeStatus.ENABLED,
                                    fields.VolumeStatus.IN_USE]:
            msg = (_('Volume to create snapshot should be enabled or in-use, '
                     'but current status is "%s".') % volume['status'])
            raise exception.InvalidVolume(reason=msg)

        snapshot = None
        replicate_mode = volume['replicate_mode']
        if checkpoint_id and replicate_mode == constants.REP_MASTER:
            destination = constants.REMOTE_SNAPSHOT
        else:
            destination = constants.LOCAL_SNAPSHOT
        host = volume['host']
        try:

            kwargs = {
                'host': host,
                'user_id': context.user_id,
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

        self.controller_rpcapi.create_snapshot(context, snapshot, volume)
        return snapshot

    def delete_snapshot(self, context, snapshot):
        if snapshot['status'] not in [fields.SnapshotStatus.AVAILABLE,
                                      fields.SnapshotStatus.ERROR]:
            msg = _('Snapshot to be deleted must be available or error')
            raise exception.InvalidSnapshot(reason=msg)

        snapshot.update({'status': fields.SnapshotStatus.DELETING})
        snapshot.save()
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
            snapshots = objects.SnapshotList.get_all(
                context, marker=marker, limit=limit, sort_keys=sort_keys,
                sort_dirs=sort_dirs, filters=filters, offset=offset)
        else:
            snapshots = objects.SnapshotList.get_all_by_project(
                context, project_id=context.project_id, marker=marker,
                limit=limit, sort_keys=sort_keys, sort_dirs=sort_dirs,
                filters=filters, offset=offset)

        LOG.info(_LI("Get all snapshots completed successfully."))
        return snapshots

    def rollback_snapshot(self, context, snapshot):
        volume = objects.Volume.get_by_id(context, snapshot['volume_id'])

        if volume["status"] not in [fields.VolumeStatus.ENABLED,
                                    fields.VolumeStatus.IN_USE]:
            msg = (_('Volume to rollback snapshot should be enabled or '
                     'in-use, but current status is "%s".') % volume['status'])
            raise exception.InvalidVolume(reason=msg)

        volume.update({"status": fields.VolumeStatus.ROLLING_BACK})
        volume.save()
        self.controller_rpcapi.rollback_snapshot(context, snapshot, volume)
        rollback = {
            'volume_id': volume['id'],
            'volume_status': volume['status'],
            'id': snapshot['id'],
        }
        return rollback

    def create_volume(self, context, snapshot=None, checkpoint=None,
                      volume_type=None, availability_zone=None, name=None,
                      description=None):
        if snapshot is not None:
            if snapshot['status'] != fields.SnapshotStatus.AVAILABLE:
                msg = (_('The specified snapshot must be available, '
                         'but current is %s'), snapshot['status'])
                raise exception.InvalidSnapshot(reason=msg)
            snapshot_availability_zone = snapshot['availability_zone']
            if availability_zone is None:
                availability_zone = snapshot_availability_zone
            if availability_zone != snapshot_availability_zone:
                msg = _("Invalid availability-zone")
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
        elif checkpoint is not None:
            if checkpoint['status'] != fields.CheckpointStatus.AVAILABLE:
                msg = (_('The specified checkpoint must be available, '
                         'but current is %s'), checkpoint['status'])
                raise exception.InvalidCheckpoint(reason=msg)
            master_snapshot = objects.Snapshot.get_by_id(
                context, checkpoint['master_snapshot'])
            slave_snapshot = objects.Snapshot.get_by_id(
                context, checkpoint['slave_snapshot'])
            if availability_zone is None:
                availability_zone = master_snapshot['availability_zone']
            if (availability_zone != master_snapshot['availability_zone'] and
                        availability_zone != slave_snapshot[
                        'availability_zone']):
                msg = _("Invalid availability-zone")
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
            if availability_zone == master_snapshot['availability_zone']:
                snapshot = master_snapshot
            else:
                snapshot = slave_snapshot
        else:
            msg = _("Create volume must specified a snapshot or checkpoint")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        volume = objects.Volume.get_by_id(context, snapshot['volume_id'])
        cinder_client = ClientFactory.create_client("cinder", context)
        try:
            cinder_volume = cinder_client.volumes.create(
                name=name,
                description=description,
                volume_type=volume_type,
                availability_zone=availability_zone,
                size=volume['size'])
        except Exception as err:
            msg = (_("Using cinder-client to create new volume failed, "
                     "err: %s."), err)
            LOG.error(msg)
            raise exception.CinderClientError(reason=msg)
        self.controller_rpcapi.create_volume(context, snapshot, cinder_volume)

    def get_replication(self, context, replication_id):
        try:
            replication = objects.Replication.get_by_id(context,
                                                        replication_id)
            LOG.info(_LI("Replication info retrieved successfully."),
                     resource=replication)
            return replication
        except Exception:
            raise exception.ReplicationNotFound(replication_id)

    def create_replication(self, context, name, description, master_volume,
                           slave_volume):
        if master_volume['status'] not in [fields.VolumeStatus.ENABLED,
                                           fields.VolumeStatus.IN_USE]:
            msg = (_('Master volume of a replication should be enabled '
                     'or in-use, but current status is "%s".') %
                   master_volume['status'])
            raise exception.InvalidVolume(reason=msg)
        if master_volume['replication_id'] is not None:
            msg = (_('Master volume already belong to one replication'))
            raise exception.InvalidVolume(reason=msg)

        if slave_volume['status'] not in [fields.VolumeStatus.ENABLED]:
            msg = (_('Slave volume of a replication should be enabled, '
                     'but current status is "%s".') %
                   slave_volume['status'])
            raise exception.InvalidVolume(reason=msg)
        if slave_volume['replication_id'] is not None:
            msg = (_('Slave volume already belong to one replication'))
            raise exception.InvalidVolume(reason=msg)

        replication = None
        try:
            kwargs = {
                'user_id': context.user_id,
                'project_id': context.project_id,
                'display_name': name,
                'display_description': description,
                'master_volume': master_volume['id'],
                'slave_volume': slave_volume['id'],
                'status': fields.ReplicateStatus.CREATING,
            }
            replication = objects.Replication(context, **kwargs)
            replication.create()
            replication.save()
        except Exception:
            with excutils.save_and_reraise_exception():
                if replication and 'id' in replication:
                    replication.destroy()

        try:
            self.create_replicate(context, master_volume, constants.REP_MASTER,
                                  replication['id'], slave_volume['id'])
            self.create_replicate(context, slave_volume, constants.REP_SLAVE,
                                  replication['id'], master_volume['id'])
        except Exception:
            with excutils.save_and_reraise_exception():
                replication.destroy()

        return replication

    def delete_replication(self, context, replication):
        if replication['status'] not in [fields.ReplicationStatus.DISABLED]:
            msg = _('Replication to be deleted must be disabled')
            raise exception.InvalidReplication(reason=msg)

        replication.update({'status': fields.ReplicationStatus.DELETING})
        replication.save()
        master_volume_id = replication['master_volume']
        master_volume = objects.Volume.get_by_id(context, master_volume_id)
        slave_volume_id = replication['slave_volume']
        slave_volume = objects.Volume.get_by_id(context, slave_volume_id)

        try:
            self.delete_replicate(context, master_volume)
            self.delete_replicate(context, slave_volume)
        except Exception:
            with excutils.save_and_reraise_exception():
                replication.update({'status': fields.ReplicationStatus.ERROR})
                replication.save()

    def get_all_replications(self, context, marker=None, limit=None,
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
            replications = objects.ReplicationList.get_all(
                context, marker=marker, limit=limit,
                sort_keys=sort_keys, sort_dirs=sort_dirs,
                filters=filters, offset=offset)
        else:
            replications = objects.ReplicationList.get_all_by_project(
                context, project_id=context.project_id, marker=marker,
                limit=limit, sort_keys=sort_keys, sort_dirs=sort_dirs,
                filters=filters, offset=offset)

        LOG.info(_LI("Get all replications completed successfully."))
        return replications

    def enable_replication(self, context, replication):
        if replication['status'] not in [fields.ReplicationStatus.DISABLED,
                                         fields.ReplicationStatus.FAILED_OVER]:
            msg = _('Replication to be enabled must be disabled or '
                    'failed-over')
            raise exception.InvalidReplication(reason=msg)

        replication.update({'status': fields.ReplicationStatus.ENABLING})
        replication.save()
        master_volume_id = replication['master_volume']
        master_volume = objects.Volume.get_by_id(context, master_volume_id)
        slave_volume_id = replication['slave_volume']
        slave_volume = objects.Volume.get_by_id(context, slave_volume_id)

        try:
            self.enable_replicate(context, master_volume)
            self.enable_replicate(context, slave_volume)
        except Exception:
            with excutils.save_and_reraise_exception():
                replication.update({'status': fields.ReplicationStatus.ERROR})
                replication.save()

        return replication

    def disable_replication(self, context, replication):
        if replication['status'] not in [fields.ReplicationStatus.ENABLED,
                                         fields.ReplicationStatus.FAILED_OVER]:
            msg = _('Replication to be disabled must be enabled or '
                    'failed-over')
            raise exception.InvalidReplication(reason=msg)

        replication.update({'status': fields.ReplicationStatus.DISABLING})
        replication.save()
        master_volume_id = replication['master_volume']
        master_volume = objects.Volume.get_by_id(context, master_volume_id)
        slave_volume_id = replication['slave_volume']
        slave_volume = objects.Volume.get_by_id(context, slave_volume_id)

        try:
            self.disable_replicate(context, master_volume)
            self.disable_replicate(context, slave_volume)
        except Exception:
            with excutils.save_and_reraise_exception():
                replication.update({'status': fields.ReplicationStatus.ERROR})
                replication.save()

        return replication

    def failover_replication(self, context, replication):
        if replication['status'] not in [fields.ReplicationStatus.ENABLED]:
            msg = _('Replication to be failed-over must be enabled')
            raise exception.InvalidReplication(reason=msg)

        replication.update({'status': fields.ReplicationStatus.FAILING_OVER})
        replication.save()

        master_volume_id = replication['master_volume']
        master_volume = objects.Volume.get_by_id(context, master_volume_id)
        slave_volume_id = replication['slave_volume']
        slave_volume = objects.Volume.get_by_id(context, slave_volume_id)

        try:
            self.failover_replicate(context, master_volume)
            self.failover_replicate(context, slave_volume)
        except Exception:
            with excutils.save_and_reraise_exception():
                replication.update({'status': fields.ReplicationStatus.ERROR})
                replication.save()

        return replication

    def reverse_replication(self, context, replication):
        if replication['status'] not in [fields.ReplicationStatus.FAILED_OVER]:
            msg = _('Replication to be reversed must be failed-over')
            raise exception.InvalidReplication(reason=msg)

        replication.update({'status': fields.ReplicationStatus.REVERSING})
        replication.save()

        master_volume_id = replication['master_volume']
        master_volume = objects.Volume.get_by_id(context, master_volume_id)
        slave_volume_id = replication['slave_volume']
        slave_volume = objects.Volume.get_by_id(context, slave_volume_id)

        try:
            self.reverse_replicate(context, master_volume)
            self.reverse_replicate(context, slave_volume)
        except Exception:
            with excutils.save_and_reraise_exception():
                replication.update({'status': fields.ReplicationStatus.ERROR})
                replication.save()

        return replication

    def create_replicate(self, context, volume, mode, replication_id,
                         peer_volume):
        if volume['replicate_status'] is not None:
            msg = (_('Replicate-status of create-replicate volume must '
                     'be None, but current status is "%s".') %
                   volume['replicate_status'])
            raise exception.InvalidVolume(reason=msg)

        if (mode == constants.REP_MASTER
            and volume['status'] not in [fields.VolumeStatus.ENABLED,
                                         fields.VolumeStatus.IN_USE]):
            msg = (_('Master volume of a replication should be enabled '
                     'or in-use, but current status is "%s".') %
                   volume['status'])
            raise exception.InvalidVolume(reason=msg)
        elif (mode == constants.REP_SLAVE
              and volume['status'] not in [fields.VolumeStatus.ENABLED]):
            msg = (_('Slave volume of a replication should be enabled, '
                     'but current status is "%s".') %
                   volume['status'])
            raise exception.InvalidVolume(reason=msg)

        if mode == constants.REP_MASTER:
            access_mode = constants.ACCESS_RW
        else:
            access_mode = constants.ACCESS_RO
        volume.update({'replicate_status': fields.ReplicateStatus.ENABLING,
                       'replication_id': replication_id,
                       'replicate_mode': mode,
                       'peer_volume': peer_volume,
                       'access_mode': access_mode})
        volume.save()

        self.controller_rpcapi.create_replicate(context, volume)
        return volume

    def enable_replicate(self, context, volume):
        if volume['replicate_status'] not in [
            fields.ReplicateStatus.DISABLED,
            fields.ReplicateStatus.FAILED_OVER
        ]:
            msg = (_('Replicate-status of enable-replicate volume must be '
                     'disabled or failed-over, but current status is "%s".') %
                   volume['replicate_status'])
            raise exception.InvalidVolume(reason=msg)

        volume.update({'replicate_status': fields.ReplicateStatus.ENABLING})
        volume.save()

        self.controller_rpcapi.enable_replicate(context, volume)
        return volume

    def disable_replicate(self, context, volume):
        if volume['replicate_status'] not in [
            fields.ReplicateStatus.ENABLED,
            fields.ReplicateStatus.FAILED_OVER
        ]:
            msg = (_('Replicate-status of disable-replicate volume must be '
                     'enabled or failed-over, but current status is "%s".') %
                   volume['replicate_status'])
            raise exception.InvalidVolume(reason=msg)

        volume.update({'replicate_status': fields.ReplicateStatus.DISABLING})
        volume.save()
        self.controller_rpcapi.disable_replicate(context, volume)
        return volume

    def delete_replicate(self, context, volume):
        if volume['replicate_status'] not in [
            fields.ReplicateStatus.DISABLED,
            fields.ReplicateStatus.FAILED_OVER,
            fields.ReplicateStatus.ERROR
        ]:
            msg = (_('Replicate-status of delete-replicate volume must be '
                     'disabled or failed-over or error, but current status '
                     'is "%s".') %
                   volume['replicate_status'])
            raise exception.InvalidVolume(reason=msg)

        volume.update({'replicate_status': fields.ReplicateStatus.DELETING})
        volume.save()
        self.controller_rpcapi.delete_replicate(context, volume)

    def failover_replicate(self, context, volume):
        if volume['replicate_status'] not in [fields.ReplicateStatus.ENABLED]:
            msg = (_('Replicate-status of failover-replicate volume must '
                     'be enabled, but current status is "%s".') %
                   volume['replicate_status'])
            raise exception.InvalidVolume(reason=msg)

        volume.update(
            {'replicate_status': fields.ReplicateStatus.FAILING_OVER})
        volume.save()
        self.controller_rpcapi.failover_replicate(context, volume)
        return volume

    def reverse_replicate(self, context, volume):
        if volume['replicate_status'] not in [
            fields.ReplicateStatus.FAILED_OVER
        ]:
            msg = (_('Replicate-status of reverse-replicate volume must '
                     'be failed-over, but current status is "%s".') %
                   volume['replicate_status'])
            raise exception.InvalidVolume(reason=msg)

        volume.update({'replicate_status': fields.ReplicateStatus.REVERSING})
        volume.save()

        self.controller_rpcapi.reverse_replicate(context, volume)
        return volume

    def get_checkpoint(self, context, checkpoint_id):
        try:
            checkpoint = objects.Checkpoint.get_by_id(context,
                                                      checkpoint_id)
            LOG.info(_LI("Checkpoint info retrieved successfully."),
                     resource=checkpoint)
            return checkpoint
        except Exception:
            raise exception.SnapshotNotFound(checkpoint_id)

    def delete_checkpoint(self, context, checkpoint):
        if checkpoint['status'] not in [fields.CheckpointStatus.AVAILABLE]:
            msg = _('Checkpoint to be deleted must be available')
            raise exception.InvalidCheckpoint(reason=msg)

        checkpoint.update({'status': fields.CheckpointStatus.DELETING})
        checkpoint.save()
        master_snapshot = objects.Snapshot.get_by_id(
            context, checkpoint['master_snapshot'])
        slave_snapshot = objects.Snapshot.get_by_id(
            context, checkpoint['slave_snapshot'])

        try:
            self.delete_snapshot(context, master_snapshot)
            self.delete_snapshot(context, slave_snapshot)
        except Exception:
            with excutils.save_and_reraise_exception():
                checkpoint.update({'status': fields.CheckpointStatus.ERROR})
                checkpoint.save()

    def create_checkpoint(self, context, name, description, replication):
        if replication['status'] not in [fields.ReplicationStatus.ENABLED]:
            msg = _('Replication to create checkpoint must be enabled')
            raise exception.InvalidReplication(reason=msg)

        checkpoint = None
        try:
            kwargs = {
                'user_id': context.user_id,
                'project_id': context.project_id,
                'display_name': name,
                'display_description': description,
                'replication_id': replication['id'],
                'status': fields.ReplicateStatus.CREATING,
            }
            checkpoint = objects.Checkpoint(context, **kwargs)
            checkpoint.create()
            checkpoint.save()
        except Exception:
            with excutils.save_and_reraise_exception():
                if checkpoint and 'id' in checkpoint:
                    checkpoint.destroy()

        try:
            snapshot_name = 'snapshot-checkpoint-%s' % checkpoint.id
            snapshot_description = snapshot_name
            slave_volume = objects.Volume.get_by_id(
                context, replication['slave_volume'])
            slave_snapshot = self.create_snapshot(context, snapshot_name,
                                                  snapshot_description,
                                                  slave_volume,
                                                  checkpoint['id'])
            master_volume = objects.Volume.get_by_id(
                context, replication['master_volume'])
            master_snapshot = self.create_snapshot(context, snapshot_name,
                                                   snapshot_description,
                                                   master_volume,
                                                   checkpoint['id'])
            checkpoint.update({'master_snapshot': master_snapshot['id'],
                               'slave_snapshot': slave_snapshot['id']})
            checkpoint.save()
            return checkpoint
        except Exception:
            with excutils.save_and_reraise_exception():
                checkpoint.update({'status': fields.CheckpointStatus.ERROR})
                checkpoint.save()

    def get_all_checkpoints(self, context, marker=None, limit=None,
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
            checkpoints = objects.CheckpointList.get_all(
                context, marker=marker, limit=limit,
                sort_keys=sort_keys, sort_dirs=sort_dirs,
                filters=filters, offset=offset)
        else:
            checkpoints = objects.CheckpointList.get_all_by_project(
                context, project_id=context.project_id, marker=marker,
                limit=limit, sort_keys=sort_keys, sort_dirs=sort_dirs,
                filters=filters, offset=offset)

        LOG.info(_LI("Get all checkpoints completed successfully."))
        return checkpoints

    def rollback_checkpoint(self, context, checkpoint):
        if checkpoint['status'] not in [fields.CheckpointStatus.ENABLED]:
            msg = _('Checkpoint to rollback must be enabled')
            raise exception.InvalidCheckpoint(reason=msg)

        master_snapshot = objects.Snapshot.get_by_id(
            context,
            checkpoint['master_snapshot'])
        slave_snapshot = objects.Snapshot.get_by_id(
            context,
            checkpoint['slave_snapshot'])
        try:
            self.rollback_snapshot(context, master_snapshot)
            self.rollback_snapshot(context, slave_snapshot)
        except Exception as err:
            msg = (_("rollback checkpoint failed, err: %s"), err)
            LOG.error(msg)
            raise exception.RollbackFailed(reason=msg)

        rollback = {
            'id': checkpoint['id'],
            'master_volume': master_snapshot['volume_id'],
            'slave_volume': slave_snapshot['volume_id']
        }
        return rollback
