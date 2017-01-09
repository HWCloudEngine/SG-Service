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

"""Tests for Models Database."""

from oslo_config import cfg
from oslo_utils import uuidutils

from sgservice import context
from sgservice import db
from sgservice import exception
from sgservice.tests import base

CONF = cfg.CONF


class ServicesDbTestCase(base.TestCase):
    """Test cases for Services database table."""

    def setUp(self):
        super(ServicesDbTestCase, self).setUp()
        self.ctxt = context.RequestContext(user_id='user_id',
                                           project_id='project_id',
                                           is_admin=True)

    def test_services_create(self):
        service_ref = db.service_create(self.ctxt,
                                        {'host': 'hosttest',
                                         'binary': 'binarytest',
                                         'topic': 'topictest',
                                         'report_count': 0})
        self.assertEqual(service_ref['host'], 'hosttest')

    def test_services_get(self):
        service_ref = db.service_create(self.ctxt,
                                        {'host': 'hosttest1',
                                         'binary': 'binarytest1',
                                         'topic': 'topictest1',
                                         'report_count': 0})

        service_get_ref = db.service_get(self.ctxt, service_ref['id'])
        self.assertEqual(service_ref['host'], 'hosttest1')
        self.assertEqual(service_get_ref['host'], 'hosttest1')

    def test_service_destroy(self):
        service_ref = db.service_create(self.ctxt,
                                        {'host': 'hosttest2',
                                         'binary': 'binarytest2',
                                         'topic': 'topictest2',
                                         'report_count': 0})
        service_id = service_ref['id']
        db.service_destroy(self.ctxt, service_id)
        self.assertRaises(exception.ServiceNotFound, db.service_get,
                          self.ctxt, service_id)

    def test_service_update(self):
        service_ref = db.service_create(self.ctxt,
                                        {'host': 'hosttest3',
                                         'binary': 'binarytest3',
                                         'topic': 'topictest3',
                                         'report_count': 0})
        service_id = service_ref['id']
        service_update_ref = db.service_update(self.ctxt, service_id,
                                               {'host': 'hosttest4',
                                                'binary': 'binarytest4',
                                                'topic': 'topictest4',
                                                'report_count': 0})
        self.assertEqual(service_ref['host'], 'hosttest3')
        self.assertEqual(service_update_ref['host'], 'hosttest4')

    def test_service_get_by_host_and_topic(self):
        service_ref = db.service_create(self.ctxt,
                                        {'host': 'hosttest5',
                                         'binary': 'binarytest5',
                                         'topic': 'topictest5',
                                         'report_count': 0})

        service_get_ref = db.service_get_by_host_and_topic(self.ctxt,
                                                           'hosttest5',
                                                           'topictest5')
        self.assertEqual(service_ref['host'], 'hosttest5')
        self.assertEqual(service_get_ref['host'], 'hosttest5')


class ReplicationsDBTestCase(base.TestCase):
    """Test cases for replications table"""

    def setUp(self):
        super(ReplicationsDBTestCase, self).setUp()
        self.ctxt = context.RequestContext(user_id='user_id',
                                           project_id='project_id',
                                           is_admin=True)

    def _create_replication(self, master_volume, slave_volume):
        values = {
            'id': uuidutils.generate_uuid(),
            'master_volume': master_volume,
            'slave_volume': slave_volume,
            'display_name': 'test replication',
            'project_id': self.ctxt.tenant,
            'display_description': 'test replication',
            'status': 'enabling',
        }
        return db.replication_create(self.ctxt, values)

    def test_replication_create(self):
        master_volume = uuidutils.generate_uuid()
        slave_volume = uuidutils.generate_uuid()
        replication_ref = self._create_replication(master_volume, slave_volume)
        self.assertEqual('enabling', replication_ref['status'])

    def test_replication_destroy(self):
        master_volume = uuidutils.generate_uuid()
        slave_volume = uuidutils.generate_uuid()
        replication_ref = self._create_replication(master_volume, slave_volume)
        db.replication_destroy(self.ctxt, replication_ref['id'])

        self.assertRaises(exception.ReplicationNotFound,
                          db.replication_get,
                          self.ctxt, replication_ref['id'])

    def test_replication_update(self):
        master_volume = uuidutils.generate_uuid()
        slave_volume = uuidutils.generate_uuid()
        replication_ref = self._create_replication(master_volume, slave_volume)
        id = replication_ref['id']
        replication_ref = db.replication_update(self.ctxt, id,
                                                {'status': 'enabled'})
        self.assertEqual('enabled', replication_ref['status'])

        replication_ref = db.replication_get(self.ctxt, id)
        self.assertEqual('enabled', replication_ref['status'])

        self.assertRaises(exception.ReplicationNotFound,
                          db.replication_update,
                          self.ctxt, '100', {"status": "enabled"})

    def test_replication_get(self):
        master_volume = uuidutils.generate_uuid()
        slave_volume = uuidutils.generate_uuid()
        replication_ref = self._create_replication(master_volume, slave_volume)
        replication_ref = db.replication_get(self.ctxt, replication_ref['id'])
        self.assertEqual('enabling', replication_ref['status'])


class VolumesDBTestCase(base.TestCase):
    """Test cases for volumes table"""

    def setUp(self):
        super(VolumesDBTestCase, self).setUp()
        self.ctxt = context.RequestContext(user_id='user_id',
                                           project_id='project_id',
                                           is_admin=True)

    def _create_volume(self):
        values = {
            'id': uuidutils.generate_uuid(),
            'project_id': self.ctxt.tenant,
            'status': 'enabling',
        }
        return db.volume_create(self.ctxt, values)

    def test_volume_create(self):
        volume_ref = self._create_volume()
        self.assertEqual('enabling', volume_ref['status'])

    def test_volume_destroy(self):
        volume_ref = self._create_volume()
        db.volume_destroy(self.ctxt, volume_ref['id'])

        self.assertRaises(exception.VolumeNotFound,
                          db.volume_get,
                          self.ctxt, volume_ref['id'])

    def test_volume_update(self):
        volume_ref = self._create_volume()
        id = volume_ref['id']
        volume_ref = db.volume_update(self.ctxt, id, {'status': 'enabled'})
        self.assertEqual('enabled', volume_ref['status'])

        volume_ref = db.volume_get(self.ctxt, id)
        self.assertEqual('enabled', volume_ref['status'])

        self.assertRaises(exception.VolumeNotFound,
                          db.volume_update,
                          self.ctxt, '100', {"status": "enabled"})

    def test_volume_get(self):
        volume_ref = self._create_volume()
        volume_ref = db.volume_get(self.ctxt, volume_ref['id'])
        self.assertEqual('enabling', volume_ref['status'])

    def test_volume_get_join_replication(self):
        def _create_replication(master_volume, slave_volume):
            values = {
                'id': uuidutils.generate_uuid(),
                'master_volume': master_volume,
                'slave_volume': slave_volume,
                'display_name': 'test replication',
                'project_id': self.ctxt.tenant,
                'display_description': 'test replication',
                'status': 'enabling',
            }
            return db.replication_create(self.ctxt, values)

        master_volume_ref = self._create_volume()
        slave_volume_ref = self._create_volume()

        replication_ref = _create_replication(master_volume_ref['id'],
                                              slave_volume_ref['id'])
        db.volume_update(self.ctxt, master_volume_ref['id'],
                         {"replicate_status": 'enabling',
                          'replication_id': replication_ref['id']})
        db.volume_update(self.ctxt, slave_volume_ref['id'],
                         {"replicate_status": 'enabling',
                          'replication_id': replication_ref['id']})

        master_volume_ref = db.volume_get(self.ctxt, master_volume_ref['id'],
                                          'replication')
        slave_volume_ref = db.volume_get(self.ctxt, slave_volume_ref['id'],
                                         'replication')
        self.assertEqual(replication_ref['id'],
                         master_volume_ref.replication['id'])
        self.assertEqual(replication_ref['id'],
                         slave_volume_ref.replication['id'])


class BackupsDBTestCase(base.TestCase):
    """Test cases for backups table"""

    def setUp(self):
        super(BackupsDBTestCase, self).setUp()
        self.ctxt = context.RequestContext(user_id='user_id',
                                           project_id='project_id',
                                           is_admin=True)

    def _create_backup(self, volume_id="bf2324be5787490b9b0a527b0dd1b737"):
        values = {
            'id': uuidutils.generate_uuid(),
            'display_name': 'test backup',
            'volume_id': volume_id,
            'project_id': self.ctxt.tenant,
            'display_description': 'test backup',
            'status': 'creating',
        }
        return db.backup_create(self.ctxt, values)

    def test_backup_create(self):
        backup_ref = self._create_backup()
        self.assertEqual('creating', backup_ref['status'])

    def test_backup_destroy(self):
        backup_ref = self._create_backup()
        db.backup_destroy(self.ctxt, backup_ref['id'])

        self.assertRaises(exception.BackupNotFound,
                          db.backup_get,
                          self.ctxt, backup_ref['id'])

    def test_backup_update(self):
        backup_ref = self._create_backup()
        id = backup_ref['id']
        backup_ref = db.backup_update(self.ctxt, id, {'status': 'enabled'})
        self.assertEqual('enabled', backup_ref['status'])

        backup_ref = db.backup_get(self.ctxt, id)
        self.assertEqual('enabled', backup_ref['status'])

        self.assertRaises(exception.BackupNotFound,
                          db.backup_update,
                          self.ctxt, '100', {"status": "enabled"})

    def test_backup_get(self):
        backup_ref = self._create_backup()
        backup_ref = db.backup_get(self.ctxt, backup_ref['id'])
        self.assertEqual('creating', backup_ref['status'])


class SnapshotsDBTestCase(base.TestCase):
    """Test cases for snapshots table"""

    def setUp(self):
        super(SnapshotsDBTestCase, self).setUp()
        self.ctxt = context.RequestContext(user_id='user_id',
                                           project_id='project_id',
                                           is_admin=True)

    def _create_snapshot(self, volume_id="bf2324be5787490b9b0a527b0dd1b737"):
        values = {
            'id': uuidutils.generate_uuid(),
            'display_name': 'test snapshot',
            'volume_id': volume_id,
            'project_id': self.ctxt.tenant,
            'display_description': 'test snapshot',
            'status': 'creating',
        }
        return db.snapshot_create(self.ctxt, values)

    def test_snapshot_create(self):
        snapshot_ref = self._create_snapshot()
        self.assertEqual('creating', snapshot_ref['status'])

    def test_snapshot_destroy(self):
        snapshot_ref = self._create_snapshot()
        db.snapshot_destroy(self.ctxt, snapshot_ref['id'])

        self.assertRaises(exception.SnapshotNotFound,
                          db.snapshot_get,
                          self.ctxt, snapshot_ref['id'])

    def test_snapshot_update(self):
        snapshot_ref = self._create_snapshot()
        id = snapshot_ref['id']
        snapshot_ref = db.snapshot_update(self.ctxt, id, {'status': 'enabled'})
        self.assertEqual('enabled', snapshot_ref['status'])

        snapshot_ref = db.snapshot_get(self.ctxt, id)
        self.assertEqual('enabled', snapshot_ref['status'])

        self.assertRaises(exception.SnapshotNotFound,
                          db.snapshot_update,
                          self.ctxt, '100', {"status": "enabled"})

    def test_snapshot_get(self):
        snapshot_ref = self._create_snapshot()
        snapshot_ref = db.snapshot_get(self.ctxt, snapshot_ref['id'])
        self.assertEqual('creating', snapshot_ref['status'])


class VolumeAttachmentDBTestCase(base.TestCase):
    """Test cases for volume attachment table"""

    def setUp(self):
        super(VolumeAttachmentDBTestCase, self).setUp()
        self.ctxt = context.RequestContext(user_id='user_id',
                                           project_id='project_id',
                                           is_admin=True)

    def _volume_attach(self, volume_id='bf2324be5787490b9b0a527b0dd1b737',
                       instance_uuid='5e7e3fd6-1bf4-4262-afa8-d88eb4bd8081',
                       mountpoint='/dev/sdb'):
        values = {
            'id': uuidutils.generate_uuid(),
            'volume_id': volume_id,
            'instance_uuid': instance_uuid,
            'host_name': 'test',
            'mountpoint': mountpoint,
            'attach_mode': 'rw',
            'attach_status': 'attaching'
        }
        return db.volume_attach(self.ctxt, values)

    def test_volume_attach(self):
        attachment_ref = self._volume_attach()
        self.assertEqual('attaching', attachment_ref['attach_status'])

    def test_volume_detach(self):
        def _create_volume():
            values = {
                'id': 'bf2324be5787490b9b0a527b0dd1b737',
                'project_id': self.ctxt.tenant,
                'status': 'attached',
            }
            return db.volume_create(self.ctxt, values)

        _create_volume()
        attachment_ref = self._volume_attach()
        db.volume_detached(self.ctxt, attachment_ref['volume_id'],
                           attachment_ref['id'])

        self.assertRaises(exception.VolumeAttachmentNotFound,
                          db.volume_attachment_get,
                          self.ctxt, attachment_ref['id'])

    def test_attachment_update(self):
        attachment_ref = self._volume_attach()
        id = attachment_ref['id']
        attachment_ref = db.volume_attachment_update(
            self.ctxt, id, {'attach_status': 'attached'})

        self.assertEqual('attached', attachment_ref['attach_status'])

        attachment_ref = db.volume_attachment_get(self.ctxt, id)
        self.assertEqual('attached', attachment_ref['attach_status'])

        self.assertRaises(exception.VolumeAttachmentNotFound,
                          db.volume_attachment_update,
                          self.ctxt, '100', {"attach_status": "attached"})

    def test_attachment_get(self):
        attachment_ref = self._volume_attach()
        attachment_ref = db.volume_attachment_get(self.ctxt,
                                                  attachment_ref['id'])
        self.assertEqual('attaching', attachment_ref['attach_status'])

    def test_attachment_get_all_by_volume_id(self):
        instance_uuid_1 = "7234b557-0e68-4c36-bdab-c7f57cefc8d6"
        instance_uuid_2 = "28748532-70aa-4d29-ab05-b700f7ccc84d"
        volume_id = "bf2324be5787490b9b0a527b0dd1b737"
        self._volume_attach(volume_id=volume_id,
                            instance_uuid=instance_uuid_1)
        self._volume_attach(volume_id=volume_id,
                            instance_uuid=instance_uuid_2)

        attachments = db.volume_attachment_get_all_by_volume_id(
            self.ctxt, volume_id)
        self.assertEqual(2, len(attachments))
