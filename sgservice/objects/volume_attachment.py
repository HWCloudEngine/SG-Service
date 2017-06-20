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

from oslo_versionedobjects import fields

from sgservice import db
from sgservice import exception
from sgservice.i18n import _
from sgservice import objects
from sgservice.objects import base


@base.SGServiceObjectRegistry.register
class VolumeAttachment(base.SGServicePersistentObject, base.SGServiceObject,
                       base.SGServiceObjectDictCompat,
                       base.SGServiceComparableObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    OPTIONAL_FIELDS = ['volume']
    obj_extra_fields = ['project_id', 'volume_host']

    fields = {
        'id': fields.UUIDField(),
        'volume_id': fields.UUIDField(),
        'instance_uuid': fields.UUIDField(nullable=True),
        'instance_host': fields.StringField(nullable=True),
        'mountpoint': fields.StringField(nullable=True),

        'attach_time': fields.DateTimeField(nullable=True),
        'detach_time': fields.DateTimeField(nullable=True),
        'attach_status': fields.StringField(nullable=True),
        'attach_mode': fields.StringField(nullable=True),
        'logical_instance_id': fields.StringField(nullable=True),

        'volume': fields.ObjectField('Volume', nullable=False),
    }

    @property
    def project_id(self):
        return self.volume.project_id

    @property
    def volume_host(self):
        return self.volume.host

    @classmethod
    def _from_db_object(cls, context, attachment, db_attachment,
                        expected_attrs=None):
        if expected_attrs is None:
            expected_attrs = []
        for name, field in attachment.fields.items():
            if name in cls.OPTIONAL_FIELDS:
                continue
            value = db_attachment.get(name)
            if isinstance(field, fields.IntegerField):
                value = value or 0
            attachment[name] = value

        if 'volume' in expected_attrs:
            db_volume = db_attachment.get('volume', None)
            if db_volume:
                attachment.volume = objects.Volume._from_db_object(
                    context, objects.Volume(), db_volume)

        attachment._context = context
        attachment.obj_reset_changes()
        return attachment

    def obj_load_attr(self, attrname):
        if attrname not in self.OPTIONAL_FIELDS:
            raise exception.ObjectActionError(
                action='obj_load_attr',
                reason=_('attribute %s not lazy-loadable') % attrname)
        if not self._context:
            raise exception.OrphanedObjectError(method='obj_load_attr',
                                                objtype=self.obj_name())

        if attrname == 'volume':
            volume = objects.Volume.get_by_id(self._context, self.volume_id)
            self.volume = volume

        self.obj_reset_changes(fields=[attrname])

    @base.remotable
    def save(self):
        updates = self.sgservice_obj_get_changes()
        if updates:
            if 'volume' in updates:
                raise exception.ObjectActionError(action='save',
                                                  reason='volume changed')

            db.volume_attachment_update(self._context, self.id, updates)
            self.obj_reset_changes()

    def finish_attach(self, mount_point):
        with self.obj_as_admin():
            db_volume, updated_values = db.volume_attached(
                self._context, self.id, mount_point)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())
        return objects.Volume._from_db_object(self._context,
                                              objects.Volume(), db_volume)

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason=_('already created'))
        updates = self.sgservice_obj_get_changes()
        with self.obj_as_admin():
            db_attachment = db.volume_attach(self._context, updates)
        self._from_db_object(self._context, self, db_attachment)

    def destroy(self):
        update_values = db.attachment_destroy(self._context, self.id)
        self.update(update_values)
        self.obj_reset_changes(update_values.keys())


@base.SGServiceObjectRegistry.register
class VolumeAttachmentList(base.ObjectListBase, base.SGServiceObject):
    VERSION = '1.0'

    fields = {
        'objects': fields.ListOfObjectsField('VolumeAttachment'),
    }

    @classmethod
    def get_all_by_volume_id(cls, context, volume_id):
        attachments = db.volume_attachment_get_all_by_volume_id(context,
                                                                volume_id)
        return base.obj_make_list(context,
                                  cls(context),
                                  objects.VolumeAttachment,
                                  attachments)

    @classmethod
    def get_all_by_host(cls, context, volume_id, host):
        attachments = db.volume_attachment_get_all_by_host(context,
                                                           volume_id,
                                                           host)
        return base.obj_make_list(context, cls(context),
                                  objects.VolumeAttachment, attachments)

    @classmethod
    def get_all_by_instance_uuid(cls, context, volume_id, instance_uuid):
        attachments = db.volume_attachment_get_all_by_instance_uuid(
            context, volume_id, instance_uuid)
        return base.obj_make_list(context, cls(context),
                                  objects.VolumeAttachment, attachments)
