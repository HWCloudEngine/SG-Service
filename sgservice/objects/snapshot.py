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
from sgservice.objects import fields as c_fields

CONF = cfg.CONF


@base.SGServiceObjectRegistry.register
class Snapshot(base.SGServicePersistentObject, base.SGServiceObject,
               base.SGServiceObjectDictCompat):
    # Version 1.0: Initial version
    VERSION = '1.0'

    OPTIONAL_FIELDS = ['volume']

    fields = {
        'id': fields.UUIDField(),
        'user_id': fields.StringField(),
        'project_id': fields.StringField(),
        'host': fields.StringField(nullable=True),
        'status': c_fields.SnapshotStatusField(nullable=True),
        'display_name': fields.StringField(nullable=True),
        'display_description': fields.StringField(nullable=True),
        'checkpoint_id': fields.StringField(nullable=True),
        'destination': fields.StringField(nullable=True),
        'availability_zone': fields.StringField(nullable=True),
        'volume_id': fields.UUIDField(nullable=True),

        'volume': fields.ObjectField('Volume', nullable=True),
    }

    # NOTE(thangp): obj_extra_fields is used to hold properties that are not
    # usually part of the model
    obj_extra_fields = ['name']

    @property
    def name(self):
        return CONF.snapshot_name_template % self.id

    @property
    def volume_name(self):
        return self.volume.name

    @classmethod
    def _get_expected_attrs(cls, context, *args, **kwargs):
        return cls.OPTIONAL_FIELDS

    @classmethod
    def _from_db_object(cls, context, snapshot, db_snapshot,
                        expected_attrs=None):
        if expected_attrs is None:
            expected_attrs = cls._get_expected_attrs(context)

        for name, field in snapshot.fields.items():
            value = db_snapshot.get(name)
            if isinstance(field, fields.IntegerField):
                value = value if value is not None else 0
            setattr(snapshot, name, value)

        if 'volume' in expected_attrs:
            db_volume = db_snapshot.get('volume')
            if db_volume:
                snapshot.volume = objects.Volume._from_db_object(
                    context, objects.Volume(), db_volume)

        snapshot._context = context
        snapshot.obj_reset_changes()
        return snapshot

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
    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason=_('already created'))
        updates = self.sgservice_obj_get_changes()
        db_snapshot = db.snapshot_create(self._context, updates)
        self._from_db_object(self._context, self, db_snapshot)

    @base.remotable
    def save(self):
        updates = self.sgservice_obj_get_changes()
        if updates:
            if 'volume' in updates:
                raise exception.ObjectActionError(action='save',
                                                  reason='volume changed')
            db.snapshot_update(self._context, self.id, updates)
        self.obj_reset_changes()

    @base.remotable
    def destroy(self):
        with self.obj_as_admin():
            updated_values = db.snapshot_destroy(self._context, self.id)
            self.update(updated_values)
            self.obj_reset_changes(updated_values.keys())

    @base.remotable_classmethod
    def get_by_id(cls, context, id):
        db_snapshot = db.snapshot_get(context, id)
        if db_snapshot:
            return cls._from_db_object(context, cls(), db_snapshot)


@base.SGServiceObjectRegistry.register
class SnapshotList(base.ObjectListBase, base.SGServiceObject):
    VERSION = '1.0'

    fields = {
        'objects': fields.ListOfObjectsField('Snapshot'),
    }

    @classmethod
    def get_all(cls, context, filters, marker=None, limit=None,
                sort_keys=None, sort_dirs=None, offset=None):
        snapshots = db.snapshot_get_all(context, filters=filters,
                                        marker=marker, limit=limit,
                                        sort_keys=sort_keys,
                                        sort_dirs=sort_dirs, offset=offset)
        return base.obj_make_list(context, cls(context), objects.Snapshot,
                                  snapshots)

    @classmethod
    def get_all_by_project(cls, context, project_id, filters, marker=None,
                           limit=None, sort_keys=None, sort_dirs=None,
                           offset=None):
        snapshots = db.snapshot_get_all_by_project(
            context, project_id, filters=filters, marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, offset=offset)
        return base.obj_make_list(context, cls(context), objects.Snapshot,
                                  snapshots)

    @classmethod
    def get_all_by_volume(cls, context, volume_id):
        snapshots = db.snapshot_get_all_by_volume(context, volume_id)
        return base.obj_make_list(context, cls(context), objects.Snapshot,
                                  snapshots)
