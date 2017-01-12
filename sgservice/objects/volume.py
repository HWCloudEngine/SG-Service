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

CONF = cfg.CONF


@base.SGServiceObjectRegistry.register
class Volume(base.SGServicePersistentObject, base.SGServiceObject,
             base.SGServiceObjectDictCompat, base.SGServiceComparableObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    OPTIONAL_FIELDS = ['replication', 'volume_attachment']

    fields = {
        'id': fields.UUIDField(),
        'host': fields.UUIDField(),
        'user_id': fields.StringField(nullable=True),
        'project_id': fields.StringField(nullable=True),
        'status': fields.StringField(nullable=True),

        'previous_status': fields.StringField(nullable=True),
        'availability_zone': fields.StringField(nullable=True),
        'replication_zone': fields.StringField(nullable=True),
        'replication_id': fields.UUIDField(nullable=True),
        'replicate_status': fields.StringField(nullable=True),
        'replicate_mode': fields.StringField(nullable=True),
        'access_mode': fields.StringField(nullable=True),

        'replication': fields.ObjectField('Replication', nullable=True),
        'volume_attachment': fields.ObjectField('VolumeAttachmentList',
                                                nullable=True),
    }

    # NOTE(thangp): obj_extra_fields is used to hold properties that are not
    # usually part of the model
    obj_extra_fields = ['name']

    @property
    def name(self):
        return CONF.volume_name_template % self.name_id

    @classmethod
    def _from_db_object(cls, context, volume, db_volume, expected_attrs=None):
        if expected_attrs is None:
            expected_attrs = []
        for name, field in volume.fields.items():
            if name in cls.OPTIONAL_FIELDS:
                continue
            value = db_volume.get(name)
            if isinstance(field, fields.IntegerField):
                value = value or 0
            volume[name] = value

        if 'volume_attachment' in expected_attrs:
            if db_volume.get('volume_attachment', None) is None:
                db_volume.volume_attachment = None
            else:
                attachments = base.obj_make_list(
                    context, objects.VolumeAttachmentList(context),
                    objects.VolumeAttachment,
                    db_volume.get('volume_attachment'))
                volume.volume_attachment = attachments
        if 'replication' in expected_attrs:
            if db_volume.get('replication', None) is None:
                db_volume.replication = None
            else:
                replication = objects.Replication(context)
                replication._from_db_object(context, replication,
                                            db_volume['replication'])
                volume.replication = replication

        volume._context = context
        volume.obj_reset_changes()
        return volume

    @base.remotable
    def create(self):
        # if self.obj_attr_is_set('id'):
        #     raise exception.ObjectActionError(action='create',
        #                                       reason=_('already created'))
        updates = self.sgservice_obj_get_changes()

        if 'replication' in updates:
            raise exception.ObjectActionError(
                action='create', reason=_('replication assigned'))

        db_volume = db.volume_create(self._context, updates)
        self._from_db_object(self._context, self, db_volume)

    @base.remotable
    def save(self):
        updates = self.sgservice_obj_get_changes()
        if updates:
            if 'replication' in updates:
                raise exception.ObjectActionError(
                    action='save', reason=_('replication changed'))
            db.volume_update(self._context, self.id, updates)
        self.obj_reset_changes()

    @base.remotable
    def destroy(self):
        with self.obj_as_admin():
            updated_values = db.volume_destroy(self._context, self.id)
            self.update(updated_values)
            self.obj_reset_changes(updated_values.keys())

    def obj_load_attr(self, attrname):
        if attrname not in self.OPTIONAL_FIELDS:
            raise exception.ObjectActionError(
                action='obj_load_attr',
                reason=_('attribute %s not lazy-loadable') % attrname)
        if not self._context:
            raise exception.OrphanedObjectError(method='obj_load_attr',
                                                objtype=self.obj_name())

        if attrname == 'replication':
            self.replication = db.replication_get(self._context,
                                                  self.replication_id)
        elif attrname == 'volume_attachment':
            attachments = objects.VolumeAttachmentList.get_all_by_volume_id(
                self._context, self.id)
            self.volume_attachment = attachments
        self.obj_reset_changes(fields=[attrname])


@base.SGServiceObjectRegistry.register
class VolumeList(base.ObjectListBase, base.SGServiceObject):
    VERSION = '1.0'

    fields = {
        'objects': fields.ListOfObjectsField('Volume'),
    }

    @classmethod
    def get_all(cls, context, marker, limit, sort_keys=None, sort_dirs=None,
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
