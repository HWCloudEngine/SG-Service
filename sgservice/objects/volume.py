#    Copyright 2015 SimpliVity Corp.
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
from oslo_versionedobjects import fields

from sgservice import db
from sgservice import exception
from sgservice.i18n import _
from sgservice import objects
from sgservice.objects import base
from sgservice.objects import fields as s_fields

CONF = cfg.CONF


class MetadataObject(dict):
    # This is a wrapper class that simulates SQLAlchemy (.*)Metadata objects to
    # maintain compatibility with older representations of Volume that some
    # drivers rely on. This is helpful in transition period while some driver
    # methods are invoked with volume versioned object and some SQLAlchemy
    # object or dict.
    def __init__(self, key=None, value=None):
        super(MetadataObject, self).__init__()
        self.key = key
        self.value = value

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        self[name] = value


@base.SGServiceObjectRegistry.register
class Volume(base.SGServicePersistentObject, base.SGServiceObject,
             base.SGServiceObjectDictCompat, base.SGServiceComparableObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    OPTIONAL_FIELDS = ['volume_attachment', 'metadata']

    fields = {
        'id': fields.UUIDField(),
        'user_id': fields.StringField(),
        'project_id': fields.StringField(),
        'host': fields.UUIDField(nullable=True),
        'status': s_fields.VolumeStatusField(nullable=True),
        'previous_status': fields.StringField(nullable=True),
        'display_name': fields.StringField(nullable=True),
        'display_description': fields.StringField(nullable=True),
        'size': fields.IntegerField(nullable=True),
        'availability_zone': fields.StringField(nullable=True),
        'replication_zone': fields.StringField(nullable=True),
        'replication_id': fields.UUIDField(nullable=True),
        'peer_volume': fields.UUIDField(nullable=True),
        'replicate_status': s_fields.ReplicateStatusField(nullable=True),
        'replicate_mode': fields.StringField(nullable=True),
        'access_mode': fields.StringField(nullable=True),
        'driver_data': fields.StringField(nullable=True),
        'snapshot_id': fields.StringField(nullable=True),
        'sg_client': fields.StringField(nullable=True),

        'metadata': fields.DictOfStringsField(nullable=True),
        'volume_attachment': fields.ObjectField('VolumeAttachmentList',
                                                nullable=True),
    }

    # NOTE(thangp): obj_extra_fields is used to hold properties that are not
    # usually part of the model
    obj_extra_fields = ['name', 'volume_metadata']

    @classmethod
    def _get_expected_attrs(cls, context, *args, **kwargs):
        expected_attrs = ['metadata']

        return expected_attrs

    @property
    def name(self):
        return CONF.volume_name_template % self.id

    @property
    def volume_metadata(self):
        md = [MetadataObject(k, v) for k, v in self.metadata.items()]
        return md

    @volume_metadata.setter
    def volume_metadata(self, value):
        md = {d['key']: d['value'] for d in value}
        self.metadata = md

    def __init__(self, *args, **kwargs):
        super(Volume, self).__init__(*args, **kwargs)
        self._orig_metadata = {}

    def obj_reset_changes(self, fields=None):
        super(Volume, self).obj_reset_changes(fields)
        self._reset_metadata_tracking(fields=fields)

    @classmethod
    def _obj_from_primitive(cls, context, objver, primitive):
        obj = super(Volume, Volume)._obj_from_primitive(context, objver,
                                                        primitive)
        obj._reset_metadata_tracking()
        return obj

    def _reset_metadata_tracking(self, fields=None):
        if fields is None or 'metadata' in fields:
            self._orig_metadata = (dict(self.metadata)
                                   if 'metadata' in self else {})

    def obj_what_changed(self):
        changes = super(Volume, self).obj_what_changed()
        if 'metadata' in self and self.metadata != self._orig_metadata:
            changes.add('metadata')
        return changes

    @classmethod
    def _from_db_object(cls, context, volume, db_volume, expected_attrs=None):
        if expected_attrs is None:
            expected_attrs = []
        for name, field in volume.fields.items():
            if name in volume.OPTIONAL_FIELDS:
                continue
            value = db_volume.get(name)
            if isinstance(field, fields.IntegerField):
                value = value or 0
            volume[name] = value

        if 'metadata' in expected_attrs:
            metadata = db_volume.get('volume_metadata', [])
            volume.metadata = {item['key']: item['value'] for item in metadata}
        if 'volume_attachment' in expected_attrs:
            if db_volume.get('volume_attachment', None) is None:
                db_volume.volume_attachment = None
            else:
                attachments = base.obj_make_list(
                    context, objects.VolumeAttachmentList(context),
                    objects.VolumeAttachment,
                    db_volume.get('volume_attachment'))
                volume.volume_attachment = attachments

        volume._context = context
        volume.obj_reset_changes()
        return volume

    @base.remotable
    def create(self):
        # if self.obj_attr_is_set('id'):
        #     raise exception.ObjectActionError(action='create',
        #                                       reason=_('already created'))
        updates = self.sgservice_obj_get_changes()

        metadata = None
        if updates and 'metadata' in updates:
            metadata = updates.pop('metadata', None)
        if updates:
            db_volume = db.volume_create(self._context, updates)
            self._from_db_object(self._context, self, db_volume)
        if metadata:
            self.metadata = db.volume_metadata_update(self._context,
                                                      self.id, metadata,
                                                      True)

    @base.remotable
    def save(self):
        updates = self.sgservice_obj_get_changes()
        if updates:
            if 'metadata' in updates:
                # Metadata items that are not specified in the
                # self.metadata will be deleted
                metadata = updates.pop('metadata', None)
                self.metadata = db.volume_metadata_update(self._context,
                                                          self.id, metadata,
                                                          True)
            if updates:
                db.volume_update(self._context, self.id, updates)
        self.obj_reset_changes()

    @base.remotable
    def destroy(self):
        with self.obj_as_admin():
            updated_values = db.volume_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @base.remotable_classmethod
    def reset(cls, context, volume_id, values):
        metadata = None
        if values and 'metadata' in values:
            metadata = values.pop('metadata')

        orm_obj = db.volume_reset(context, volume_id, values)
        volume = cls._from_db_object(context, cls(context), orm_obj)
        volume.metadata = db.volume_metadata_update(context, volume.id,
                                                    metadata, True)
        return volume

    def obj_load_attr(self, attrname):
        if attrname not in self.OPTIONAL_FIELDS:
            raise exception.ObjectActionError(
                action='obj_load_attr',
                reason=_('attribute %s not lazy-loadable') % attrname)
        if not self._context:
            raise exception.OrphanedObjectError(method='obj_load_attr',
                                                objtype=self.obj_name())

        if attrname == 'metadata':
            self.metadata = db.volume_metadata_get(self._context, self.id)
        elif attrname == 'volume_attachment':
            attachments = objects.VolumeAttachmentList.get_all_by_volume_id(
                self._context, self.id)
            self.volume_attachment = attachments
        self.obj_reset_changes(fields=[attrname])

    def delete_metadata_key(self, key):
        db.volume_metadata_delete(self._context, self.id, key)
        md_was_changed = 'metadata' in self.obj_what_changed()

        del self.metadata[key]
        self._orig_metadata.pop(key, None)

        if not md_was_changed:
            self.obj_reset_changes(['metadata'])

    def update_metadata(self, metadata, delete=False):
        self.metadata = db.volume_metadata_update(self._context, self.id,
                                                  metadata, delete)

    def begin_attach(self, instance_uuid, instance_host, attach_mode,
                     logical_instance_id):
        attachment = objects.VolumeAttachment(
            context=self._context,
            attach_status=s_fields.VolumeAttachStatus.ATTACHING,
            volume_id=self.id,
            attach_mode=attach_mode,
            instance_uuid=instance_uuid,
            instance_host=instance_host,
            logical_instance_id=logical_instance_id)
        attachment.create()
        return attachment

    def finish_detach(self, attachment_id):
        with self.obj_as_admin():
            volume_updates, attachment_updates = (
                db.volume_detached(self._context, self.id, attachment_id))
        # Remove attachment in volume only when this field is loaded.
        if attachment_updates and self.obj_attr_is_set('volume_attachment'):
            for i, attachment in enumerate(self.volume_attachment):
                if attachment.id == attachment_id:
                    del self.volume_attachment.objects[i]
                    break

        self.update(volume_updates)
        self.obj_reset_changes(
            list(volume_updates.keys()) +
            ['volume_attachment'])


@base.SGServiceObjectRegistry.register
class VolumeList(base.ObjectListBase, base.SGServiceObject):
    VERSION = '1.0'

    fields = {
        'objects': fields.ListOfObjectsField('Volume'),
    }

    @classmethod
    def get_all(cls, context, marker=None, limit=None, sort_keys=None,
                sort_dirs=None,
                filters=None, offset=None):
        volumes = db.volume_get_all(context, marker=marker, limit=limit,
                                    sort_keys=sort_keys, sort_dirs=sort_dirs,
                                    filters=filters, offset=offset)
        return base.obj_make_list(context, cls(context), objects.Volume,
                                  volumes)

    @classmethod
    def get_all_by_project(cls, context, project_id, marker, limit,
                           sort_keys=None, sort_dirs=None, filters=None,
                           offset=None):
        volumes = db.volume_get_all_by_project(
            context, project_id=project_id, marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset)
        return base.obj_make_list(context, cls(context), objects.Volume,
                                  volumes)
