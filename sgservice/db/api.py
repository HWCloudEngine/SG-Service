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

"""Defines interface for DB access.

Functions in this module are imported into the sgservice.db namespace. Call
these functions from sgservice.db namespace, not the sgservice.db.api
namespace.

All functions in this module return objects that implement a dictionary-like
interface. Currently, many of these objects are sqlalchemy objects that
implement a dictionary interface. However, a future goal is to have all of
these objects be simple dictionaries.


**Related Flags**

:connection:  string specifying the sqlalchemy connection to use, like:
              `sqlite:///var/lib/sgs/sgservice.sqlite`.

:enable_new_services:  when adding a new service to the database, is it in the
                       pool of available hardware (Default: True)

"""

from oslo_config import cfg
from oslo_db import concurrency as db_concurrency
from oslo_db import options as db_options

db_opts = [
    cfg.BoolOpt('enable_new_services',
                default=True,
                help='Services to be added to the available pool on create'),
    cfg.StrOpt('volume_name_template',
               default='volume-%s',
               help='Template string to be used to generate volume names'),
    cfg.StrOpt('backup_name_template',
               default='backup-%s',
               help='Template string to be used to generate backup names'),
    cfg.StrOpt('replication_name_template',
               default='replication-%s',
               help='Template string to be used to generate replication '
                    'names'),
    cfg.StrOpt('checkpoint_name_template',
               default='checkpoint-%s',
               help='Template string to be used to generate checkpoint names'),
    cfg.StrOpt('snapshot_name_template',
               default='snapshot-%s',
               help='Template string to be used to generate snapshot names'),
]

CONF = cfg.CONF
CONF.register_opts(db_opts)
db_options.set_defaults(CONF)

_BACKEND_MAPPING = {'sqlalchemy': 'sgservice.db.sqlalchemy.api'}

IMPL = db_concurrency.TpoolDbapiWrapper(CONF, _BACKEND_MAPPING)

# The maximum value a signed INT type may have
MAX_INT = 0x7FFFFFFF


###################

def dispose_engine():
    """Force the engine to establish new connections."""

    # FIXME(jdg): When using sqlite if we do the dispose
    # we seem to lose our DB here.  Adding this check
    # means we don't do the dispose, but we keep our sqlite DB
    # This likely isn't the best way to handle this

    if 'sqlite' not in IMPL.get_engine().name:
        return IMPL.dispose_engine()
    else:
        return


###################


def service_destroy(context, service_id):
    """Destroy the service or raise if it does not exist."""
    return IMPL.service_destroy(context, service_id)


def service_get(context, service_id):
    """Get a service or raise if it does not exist."""
    return IMPL.service_get(context, service_id)


def service_get_by_host_and_topic(context, host, topic):
    """Get a service by host it's on and topic it listens to."""
    return IMPL.service_get_by_host_and_topic(context, host, topic)


def service_get_all(context, disabled=None):
    """Get all services."""
    return IMPL.service_get_all(context, disabled)


def service_get_all_by_topic(context, topic, disabled=None):
    """Get all services for a given topic."""
    return IMPL.service_get_all_by_topic(context, topic, disabled=disabled)


def service_get_by_args(context, host, binary):
    """Get the state of an service by node name and binary."""
    return IMPL.service_get_by_args(context, host, binary)


def service_create(context, values):
    """Create a service from the values dictionary."""
    return IMPL.service_create(context, values)


def service_update(context, service_id, values):
    """Set the given properties on an service and update it.

    Raises NotFound if service does not exist.

    """
    return IMPL.service_update(context, service_id, values)


def get_by_id(context, model, id, *args, **kwargs):
    return IMPL.get_by_id(context, model, id, *args, **kwargs)


###################


def backup_create(context, values):
    """Create a backup from the values dictionary.

    :param context: The security context
    :param values: Dictionary containing backup properties

    :returns: Dictionary-like object containing the properties of the created
              backup
    """
    return IMPL.backup_create(context, values)


def backup_update(context, id, values):
    """Set the given properties on a backup and update it.

    :param context: The security context
    :param id: ID of the backup
    :param values: Dictionary containing backup properties to be updated

    :returns: Dictionary-like object containing the properties of the updated
              backup

    Raises BackupNotFound if backup with the given ID doesn't exist.
    """
    return IMPL.backup_update(context, id, values)


def backup_destroy(context, id):
    """Delete a backup from the database.

    :param context: The security context
    :param id: ID of the backup

    Raises BackupNotFound if backup with the given ID doesn't exist.
    """
    return IMPL.backup_destroy(context, id)


def backup_get(context, id):
    """Get a backup by its id.

    :param context: The security context
    :param id: ID of the backup

    :returns: Dictionary-like object containing properties of the backup

    Raises BackupNotFound if backup with the given ID doesn't exist.
    """
    return IMPL.backup_get(context, id)


def backup_get_all(context, filters=None, marker=None, limit=None,
                   offset=None, sort_keys=None, sort_dirs=None):
    """Get all backups that match all filters sorted by multiple keys.

    sort_keys and sort_dirs must be a list of strings.
    """
    return IMPL.backup_get_all(context, filters=filters, marker=marker,
                               limit=limit, offset=offset, sort_keys=sort_keys,
                               sort_dirs=sort_dirs)


def backup_get_all_by_project(context, project_id, filters=None, marker=None,
                              limit=None, offset=None, sort_keys=None,
                              sort_dirs=None):
    """Get all backups belonging to a project."""
    return IMPL.backup_get_all_by_project(context, project_id,
                                          filters=filters, marker=marker,
                                          limit=limit, offset=offset,
                                          sort_keys=sort_keys,
                                          sort_dirs=sort_dirs)


def backup_get_all_by_volume(context, volume_id, filters=None):
    """Get all backups belonging to a volume."""
    return IMPL.backup_get_all_by_volume(context, volume_id,
                                         filters=filters)


###################


def replication_create(context, values):
    """Create a replication from the values dictionary.

    :param context: The security context
    :param values: Dictionary containing replication properties

    :returns: Dictionary-like object containing the properties of the created
              replication
    """
    return IMPL.replication_create(context, values)


def replication_update(context, id, values):
    """Set the given properties on a replication and update it.

    :param context: The security context
    :param id: ID of the replication
    :param values: Dictionary containing replication properties to be updated

    :returns: Dictionary-like object containing the properties of the updated
              replication

    Raises ReplicationNotFound if replication with the given ID doesn't exist.
    """
    return IMPL.replication_update(context, id, values)


def replication_destroy(context, id):
    """Delete a replication from the database.

    :param context: The security context
    :param id: ID of the replication

    Raises ReplicationNotFound if replication with the given ID doesn't exist.
    """
    return IMPL.replication_destroy(context, id)


def replication_get(context, id):
    """Get a replication by its id.

    :param context: The security context
    :param id: ID of the replication

    :returns: Dictionary-like object containing properties of the replication

    Raises ReplicationNotFound if replication with the given ID doesn't exist.
    """
    return IMPL.replication_get(context, id)


def replication_get_all(context, filters=None, marker=None, limit=None,
                        offset=None, sort_keys=None, sort_dirs=None):
    """Get all replications that match all filters sorted by multiple keys.

    sort_keys and sort_dirs must be a list of strings.
    """
    return IMPL.replication_get_all(context, filters=filters, marker=marker,
                                    limit=limit, offset=offset,
                                    sort_keys=sort_keys,
                                    sort_dirs=sort_dirs)


def replication_get_all_by_project(context, project_id, filters=None,
                                   marker=None, limit=None, offset=None,
                                   sort_keys=None, sort_dirs=None):
    """Get all replications belonging to a project."""
    return IMPL.replication_get_all_by_project(context, project_id,
                                               filters=filters, marker=marker,
                                               limit=limit, offset=offset,
                                               sort_keys=sort_keys,
                                               sort_dirs=sort_dirs)


###################


def snapshot_create(context, values):
    """Create a snapshot from the values dictionary.

    :param context: The security context
    :param values: Dictionary containing snapshot properties

    :returns: Dictionary-like object containing the properties of the created
              snapshot
    """
    return IMPL.snapshot_create(context, values)


def snapshot_update(context, id, values):
    """Set the given properties on a snapshot and update it.

    :param context: The security context
    :param id: ID of the snapshot
    :param values: Dictionary containing snapshot properties to be updated

    :returns: Dictionary-like object containing the properties of the updated
              snapshot

    Raises SnapshotNotFound if snapshot with the given ID doesn't exist.
    """
    return IMPL.snapshot_update(context, id, values)


def snapshot_destroy(context, id):
    """Delete a snapshot from the database.

    :param context: The security context
    :param id: ID of the snapshot

    Raises SnapshotNotFound if snapshot with the given ID doesn't exist.
    """
    return IMPL.snapshot_destroy(context, id)


def snapshot_get(context, id):
    """Get a snapshot by its id.

    :param context: The security context
    :param id: ID of the snapshot

    :returns: Dictionary-like object containing properties of the snapshot

    Raises SnapshotNotFound if snapshot with the given ID doesn't exist.
    """
    return IMPL.snapshot_get(context, id)


def snapshot_get_all(context, filters=None, marker=None, limit=None,
                     offset=None, sort_keys=None, sort_dirs=None):
    """Get all snapshots that match all filters sorted by multiple keys.

    sort_keys and sort_dirs must be a list of strings.
    """
    return IMPL.snapshot_get_all(context, filters=filters, marker=marker,
                                 limit=limit, offset=offset,
                                 sort_keys=sort_keys, sort_dirs=sort_dirs)


def snapshot_get_all_by_project(context, project_id, filters=None, marker=None,
                                limit=None, offset=None, sort_keys=None,
                                sort_dirs=None):
    """Get all snapshots belonging to a project."""
    return IMPL.snapshot_get_all_by_project(context, project_id,
                                            filters=filters, marker=marker,
                                            limit=limit, offset=offset,
                                            sort_keys=sort_keys,
                                            sort_dirs=sort_dirs)


def snapshot_get_all_by_volume(context, volume_id, filters=None):
    """Get all snapshots belonging to a volume."""
    return IMPL.snapshot_get_all_by_volume(context, volume_id,
                                           filters=filters)


def snapshot_get_all_by_checkpoint(context, checkpoint_id, filters=None):
    """Get all snapshots belonging to a checkpoint."""
    return IMPL.snapshot_get_all_by_checkpoint(context, checkpoint_id,
                                               filters=filters)


###################


def volume_create(context, values):
    """Create a volume from the values dictionary.

    :param context: The security context
    :param values: Dictionary containing volume properties

    :returns: Dictionary-like object containing the properties of the created
              volume
    """
    return IMPL.volume_create(context, values)


def volume_reset(context, id, values):
    """Reset a deleted volume and set the given properties update it .

    :param context: The security context
    :param id: ID of the volume
    :param values: Dictionary containing snapshot properties to be updated

    :returns: Dictionary-like object containing the properties of the created
              volume

    Raises VolumeNotFound if volume with the given ID doesn't exist.
    """
    return IMPL.volume_reset(context, id, values)


def volume_update(context, id, values):
    """Set the given properties on a volume and update it.

    :param context: The security context
    :param id: ID of the volume
    :param values: Dictionary containing snapshot properties to be updated

    :returns: Dictionary-like object containing the properties of the updated
              volume

    Raises VolumeNotFound if volume with the given ID doesn't exist.
    """
    return IMPL.volume_update(context, id, values)


def volume_destroy(context, id):
    """Delete a volume from the database.

    :param context: The security context
    :param id: ID of the volume

    Raises VolumeNotFound if volume with the given ID doesn't exist.
    """
    return IMPL.volume_destroy(context, id)


def volume_get(context, id, read_deleted=None, columns_to_join=[]):
    """Get a volume by its id.

    :param context: The security context
    :param id: ID of the volume
    :columns_to_join: columns which will be joined

    :returns: Dictionary-like object containing properties of the volume

    Raises VolumeNotFound if volume with the given ID doesn't exist.
    """
    return IMPL.volume_get(context, id, read_deleted, columns_to_join)


def volume_get_all(context, filters=None, marker=None, limit=None,
                   offset=None, sort_keys=None, sort_dirs=None):
    """Get all volumes that match all filters sorted by multiple keys.

    sort_keys and sort_dirs must be a list of strings.
    """
    return IMPL.volume_get_all(context, filters=filters, marker=marker,
                               limit=limit, offset=offset,
                               sort_keys=sort_keys, sort_dirs=sort_dirs)


def volume_get_all_by_project(context, project_id, filters=None, marker=None,
                              limit=None, offset=None, sort_keys=None,
                              sort_dirs=None):
    """Get all volumes belonging to a project."""
    return IMPL.volume_get_all_by_project(context, project_id,
                                          filters=filters, marker=marker,
                                          limit=limit, offset=offset,
                                          sort_keys=sort_keys,
                                          sort_dirs=sort_dirs)


###################


def volume_attach(context, values):
    """Attach a volume."""
    return IMPL.volume_attach(context, values)


def volume_attached(context, attachment_id, mountpoint):
    """Ensure that a volume is set as attached."""
    return IMPL.volume_attached(context, attachment_id, mountpoint)


def volume_attachment_get(context, attachment_id, session=None):
    return IMPL.volume_attachment_get(context, attachment_id, session)


def volume_attachment_get_all_by_volume_id(context, volume_id):
    return IMPL.volume_attachment_get_all_by_volume_id(context, volume_id)


def volume_attachment_get_all_by_host(context, volume_id, host):
    return IMPL.volume_attachment_get_all_by_host(context, volume_id, host)


def volume_attachment_get_all_by_instance_uuid(context,
                                               volume_id,
                                               instance_uuid):
    return IMPL.volume_attachment_get_all_by_instance_uuid(context, volume_id,
                                                           instance_uuid)


def volume_attachment_update(context, attachment_id, values):
    return IMPL.volume_attachment_update(context, attachment_id, values)


def volume_detached(context, volume_id, attachment_id):
    """Ensure that a volume is set as detached."""
    return IMPL.volume_detached(context, volume_id, attachment_id)


def attachment_destroy(context, attachment_id):
    """Destroy the attachment or raise if it does not exist"""
    return IMPL.attachment_destroy(context, attachment_id)


##################

def checkpoint_create(context, values):
    """Create a checkpoint from the values dictionary.

    :param context: The security context
    :param values: Dictionary containing checkpoint properties

    :returns: Dictionary-like object containing the properties of the created
              checkpoint
    """
    return IMPL.checkpoint_create(context, values)


def checkpoint_update(context, id, values):
    """Set the given properties on a checkpoint and update it.

    :param context: The security context
    :param id: ID of the checkpoint
    :param values: Dictionary containing checkpoint properties to be updated

    :returns: Dictionary-like object containing the properties of the updated
              checkpoint

    Raises CheckpointNotFound if checkpoint with the given ID doesn't exist.
    """
    return IMPL.checkpoint_update(context, id, values)


def checkpoint_destroy(context, id):
    """Delete a checkpoint from the database.

    :param context: The security context
    :param id: ID of the checkpoint

    Raises CheckpointNotFound if checkpoint with the given ID doesn't exist.
    """
    return IMPL.checkpoint_destroy(context, id)


def checkpoint_get(context, id):
    """Get a checkpoint by its id.

    :param context: The security context
    :param id: ID of the checkpoint

    :returns: Dictionary-like object containing properties of the checkpoint

    Raises CheckpointNotFound if checkpoint with the given ID doesn't exist.
    """
    return IMPL.checkpoint_get(context, id)


def checkpoint_get_all(context, filters=None, marker=None, limit=None,
                       offset=None, sort_keys=None, sort_dirs=None):
    """Get all checkpoints that match all filters sorted by multiple keys.

    sort_keys and sort_dirs must be a list of strings.
    """
    return IMPL.checkpoint_get_all(context, filters=filters, marker=marker,
                                   limit=limit, offset=offset,
                                   sort_keys=sort_keys,
                                   sort_dirs=sort_dirs)


def checkpoint_get_all_by_project(context, project_id, filters=None,
                                  marker=None,
                                  limit=None, offset=None, sort_keys=None,
                                  sort_dirs=None):
    """Get all checkpoints belonging to a project."""
    return IMPL.checkpoint_get_all_by_project(context, project_id,
                                              filters=filters, marker=marker,
                                              limit=limit, offset=offset,
                                              sort_keys=sort_keys,
                                              sort_dirs=sort_dirs)


def checkpoint_get_all_by_replication(context, replication_id, filters=None,):
    """Get all checkpoints belonging to a replication."""
    return IMPL.checkpoint_get_all_by_replication(
        context, replication_id, filters=filters)


###############


def volume_metadata_get(context, volume_id):
    """Get all metadata for a volume."""
    return IMPL.volume_metadata_get(context, volume_id)


def volume_metadata_delete(context, volume_id, key):
    """Delete the given metadata item."""
    return IMPL.volume_metadata_delete(context, volume_id, key)


def volume_metadata_update(context, volume_id, metadata, delete):
    """Update metadata if it exists, otherwise create it."""
    return IMPL.volume_metadata_update(context, volume_id, metadata, delete)
