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

import six

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_service import loopingcall
from oslo_utils import excutils
from oslo_utils import uuidutils

from sgservice.controller.client_factory import ClientFactory
from sgservice.controller.sgdriver import SGDriver
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
        self.driver = SGDriver()

        self.enable_volumes = {}  # used to sync enable-volumes from cinder
        sync_enable_volume_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_enable_volume)
        sync_enable_volume_loop.start(interval=self.sync_status_interval,
                                      initial_delay=self.sync_status_interval)

        self.disable_volumes = {}  # used to sync disable-volumes from cinder
        sync_disable_volume_loop = loopingcall.FixedIntervalLoopingCall(
            self._sync_disable_volume)
        sync_disable_volume_loop.start(interval=self.sync_status_interval,
                                       initial_delay=self.sync_status_interval)

        self.backups = {}  # used to sync backups from sg-client
        self.snapshots = {}  # used to sync snapshots from sg-client

    def init_host(self, **kwargs):
        """Handle initialization if this is a standalone service"""
        LOG.info(_LI("Starting controller service"))

    def _sync_enable_volume(self):
        for volume_id, volume_info in self.enable_volumes.items():
            cinder_client = volume_info['cinder_client']
            sg_volume = volume_info['sg_volume']
            old_devices = volume_info['devices']
            try:
                cinder_volume = cinder_client.volumes.get(volume_id)
                if cinder_volume.status == 'in-use':
                    LOG.info(_("Attach cinder volume '%s' to SG succeed."),
                             volume_id)
                    new_devices = self.driver.list_devices()
                    device = [d for d in new_devices if d not in old_devices]
                    if len(device) == 0:
                        msg = _("Get volume device failed")
                        LOG.error(msg)
                        sg_volume.update(
                            {'status': fields.VolumeStatus.ERROR})
                        sg_volume.save()
                    else:
                        self.driver.enable_sg(sg_volume, device[0])
                        sg_volume.update(
                            {'status': fields.VolumeStatus.ENABLED})
                        sg_volume.save()
                    self.enable_volumes.pop(volume_id)
                elif cinder_volume.status == 'attaching':
                    continue
                else:
                    LOG.info(_("Attach cinder volume '%s' to SG failed."),
                             volume_id)
                    sg_volume.update(
                        {'status': fields.VolumeStatus.ERROR})
                    self.enable_volumes.pop(volume_id)
            except exception.SGDriverError as err:
                LOG.error(err)
                sg_volume.update({'status': fields.VolumeStatus.ERROR})
                sg_volume.save()
                self.enable_volumes.pop(volume_id)
            except Exception:
                LOG.error(_LE("Sync cinder volume '%s' status failed."),
                          volume_id)
                sg_volume.destroy()
                self.enable_volumes.pop(volume_id)

    def _sync_disable_volume(self):
        for volume_id, volume_info in self.disable_volumes.items():
            cinder_client = volume_info['cinder_client']
            sg_volume = volume_info['sg_volume']
            try:
                cinder_volume = cinder_client.volumes.get(volume_id)
                if cinder_volume.status == 'available':
                    LOG.info(_("Detach cinder volume '%s' from SG succeed."),
                             volume_id)
                    self.driver.disable_sg(volume_id)
                    sg_volume.destroy()
                    self.disable_volumes.pop(volume_id)
                elif cinder_volume.status == 'detaching':
                    continue
                else:
                    LOG.info(_("Disable cinder volume '%s' SG failed."),
                             volume_id)
                    if cinder_volume.status == 'in-use':
                        sg_volume.update(
                            {'status': fields.VolumeStatus.ENABLED})
                    else:
                        sg_volume.update({'status': fields.VolumeStatus.ERROR})
                    sg_volume.save()
                    self.disable_volumes.pop(volume_id)
            except exception.SGDriverError as err:
                LOG.error(err)
                sg_volume.update({'status': fields.VolumeStatus.ERROR})
                sg_volume.save()
                self.disable_volumes.pop(volume_id)
            except Exception:
                LOG.error(_LE("Sync cinder volume '%s' status failed."),
                          volume_id)
                sg_volume.update({'status': fields.VolumeStatus.ERROR})
                sg_volume.save()
                self.disable_volumes.pop(volume_id)

    def enable_sg(self, context, volume_id):
        LOG.info(_LI("Enable-SG for this volume with id %s"), volume_id)

        volume = objects.Volume.get_by_id(context, volume_id)
        volume.update({'replication_zone': CONF.sg_client.replication_zone})
        volume.save()

        nova_client = ClientFactory.create_client('nova', context)
        cinder_client = ClientFactory.create_client('cinder', context)

        try:
            devices = self.driver.list_devices()
            nova_client.volumes.create_server_volume(
                CONF.sg_client.sg_client_instance, volume_id)
        except Exception as err:
            LOG.error(err)
            volume.update({'status': fields.VolumeStatus.ERROR})
            raise exception.EnableSGFailed(reason=err)

        self.enable_volumes[volume_id] = {
            'devices': devices,
            'cinder_client': cinder_client,
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

    def attach_volume(self, context, volume_id, instance_uuid, host_name,
                      mountpoint, mode):
        LOG.info(_LI("Attach volume '%s' to '%s'"), (volume_id, instance_uuid))

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
                                      {'status': 'available'})
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

    def create_backup(self, context, backup_id):
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
            self.driver.create_backup(backup=backup, volume=volume)
        except exception.SGDriverError:
            with excutils.save_and_reraise_exception():
                self._update_backup_error(backup)
                self.db.volume_update(context, volume_id,
                                      {'status': previous_status,
                                       'previous_status': 'error_backing_up'})

        self.backups[backup_id] = {
            'id': backup_id,
            'action': 'create'
        }

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
            self._update_backup_error(backup)
            raise exception.InvalidBackup(reason=msg)

        try:
            self.driver.delete_backup(backup=backup)
        except exception.SGDriverError:
            with excutils.save_and_reraise_exception():
                self._update_backup_error(backup)

        self.backups[backup_id] = {
            'id': backup_id,
            'action': 'delete'
        }

    def restore_backup(self, context, backup_id, volume_id):
        LOG.info(_LI("Restore backup started, backup:%(backup_id)s, volume: "
                     "%(volume_id)s"),
                 {'volume_id': volume_id, 'backup_id': backup_id})

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
            self.driver.restore_backup(backup=backup, volume=volume)
        except exception.SGDriverError:
            with excutils.save_and_reraise_exception():
                self.db.backup_update(
                    context, backup_id,
                    {'status': fields.BackupStatus.AVAILABLE})
                self.db.volume_update(
                    context, volume_id,
                    {'status': fields.VolumeStatus.ERROR_RESTORING})

        self.backups[backup_id] = {
            'id': backup_id,
            'action': 'restore'
        }

    def _update_snapshot_error(self, snapshot):
        snapshot.update({'status': fields.SnapshotStatus.ERROR})
        snapshot.save()

    def create_snapshot(self, context, snapshot_id):
        snapshot = objects.Snapshot.get_by_id(context, snapshot_id)
        volume_id = snapshot.volume_id
        LOG.info(_LI("Create snapshot started, snapshot:%(snapshot_id)s, "
                     "volume: %(volume_id)s"),
                 {'volume_id': volume_id, 'snapshot_id': snapshot_id})

        volume = objects.Volume.get_by_id(context, volume_id)

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
            self.driver.create_snapshot(snapshot=snapshot, volume=volume)
        except exception.SGDriverError:
            with excutils.save_and_reraise_exception():
                self._update_snapshot_error(snapshot)

        self.snapshots[snapshot_id] = {
            'id': snapshot_id,
            'action': 'create'
        }

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
            self._update_snapshot_error(snapshot)
            raise exception.InvalidBackup(reason=msg)

        try:
            self.driver.delete_snapshot(snapshot=snapshot)
        except exception.SGDriverError:
            with excutils.save_and_reraise_exception():
                self._update_snapshot_error(snapshot)

        self.snapshots[snapshot_id] = {
            'id': snapshot_id,
            'action': 'delete'
        }
