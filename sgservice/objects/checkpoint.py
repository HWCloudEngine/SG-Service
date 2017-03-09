#    Copyright 2015 Intel Corporation
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
from sgservice import objects
from sgservice.objects import base
from sgservice.objects import fields as s_fields
from sgservice.i18n import _

CONF = cfg.CONF


@base.SGServiceObjectRegistry.register
class Checkpoint(base.SGServicePersistentObject, base.SGServiceObject,
                 base.SGServiceObjectDictCompat):
    VERSION = '1.0'

    OPTIONAL_FIELDS = ['replication']

    fields = {
        'id': fields.UUIDField(),
        'user_id': fields.StringField(),
        'project_id': fields.StringField(),
        'host': fields.StringField(nullable=True),
        'status': s_fields.CheckpointStatusField(nullable=True),
        'display_name': fields.StringField(nullable=True),
        'display_description': fields.StringField(nullable=True),
        'replication_id': fields.UUIDField(nullable=True),
        'master_snapshot': fields.UUIDField(nullable=True),
        'slave_snapshot': fields.UUIDField(nullable=True),

        'replication': fields.ObjectField('Replication', nullable=True)
    }

    # obj_extra_fields is used to hold properties that are not
    # usually part of the model
    obj_extra_fields = ['name']

    @property
    def name(self):
        return CONF.checkpoint_name_template % self.id

    @classmethod
    def _from_db_object(cls, context, checkpoint, db_checkpoint,
                        expected_attrs=None):
        if expected_attrs is None:
            expected_attrs = []
        for name, field in checkpoint.fields.items():
            if name in checkpoint.OPTIONAL_FIELDS:
                continue
            value = db_checkpoint.get(name)
            if isinstance(field, fields.IntegerField):
                value = value if value is not None else 0
            checkpoint[name] = value

        if 'replication' in expected_attrs:
            db_replication = db_checkpoint.get('replication', None)
            if db_replication:
                checkpoint.replication = objects.Replication._from_db_object(
                    context, objects.Replication(), db_replication)

        checkpoint._context = context
        checkpoint.obj_reset_changes()
        return checkpoint

    def obj_load_attr(self, attrname):
        if attrname not in self.OPTIONAL_FIELDS:
            raise exception.ObjectActionError(
                action='obj_load_attr',
                reason=_('attribute %s not lazy-loadable') % attrname)
        if not self._context:
            raise exception.OrphanedObjectError(method='obj_load_attr',
                                                objtype=self.obj_name())

        if attrname == 'replication':
            replication = objects.Replication.get_by_id(self._context,
                                                        self.replication_id)
            self.replication = replication

        self.obj_reset_changes(fields=[attrname])

    @base.remotable
    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.sgservice_obj_get_changes()

        db_checkpoint = db.checkpoint_create(self._context, updates)
        self._from_db_object(self._context, self, db_checkpoint)

    @base.remotable
    def save(self):
        updates = self.sgservice_obj_get_changes()
        if updates:
            if 'replication' in updates:
                raise exception.ObjectActionError(action='save',
                                                  reason='replication changed')

            db.checkpoint_update(self._context, self.id, updates)
        self.obj_reset_changes()

    @base.remotable
    def destroy(self):
        with self.obj_as_admin():
            updated_values = db.checkpoint_destroy(self._context, self.id)
            self.update(updated_values)
            self.obj_reset_changes(updated_values.keys())


@base.SGServiceObjectRegistry.register
class CheckpointList(base.ObjectListBase, base.SGServiceObject):
    VERSION = '1.0'

    fields = {
        'objects': fields.ListOfObjectsField('Checkpoint'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        checkpoints = db.checkpoint_get_all(
            context, filters=filters, marker=marker, limit=limit,
            offset=offset, sort_keys=sort_keys, sort_dirs=sort_dirs)
        return base.obj_make_list(context, cls(context), objects.Checkpoint,
                                  checkpoints)

    @classmethod
    def get_all_by_project(cls, context, project_id, filters=None,
                           marker=None, limit=None, offset=None,
                           sort_keys=None, sort_dirs=None):
        checkpoints = db.checkpoint_get_all_by_project(
            context, project_id=project_id, filters=filters, marker=marker,
            limit=limit, offset=offset, sort_keys=sort_keys,
            sort_dirs=sort_dirs)
        return base.obj_make_list(context, cls(context), objects.Checkpoint,
                                  checkpoints)
