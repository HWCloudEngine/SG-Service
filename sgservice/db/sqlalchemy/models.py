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
"""
SQLAlchemy models for sgservice data.
"""

from oslo_config import cfg
from oslo_db.sqlalchemy import models
from oslo_utils import timeutils
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import DateTime, Boolean, ForeignKey
from sqlalchemy import orm

CONF = cfg.CONF
BASE = declarative_base()


class SGServiceBase(models.TimestampMixin,
                    models.ModelBase):
    """Base class for sgservice Models."""

    __table_args__ = {'mysql_engine': 'InnoDB'}

    deleted_at = Column(DateTime)
    deleted = Column(Boolean, default=False)
    metadata = None

    @staticmethod
    def delete_values():
        return {'deleted': True,
                'deleted_at': timeutils.utcnow()}

    def delete(self, session):
        """Delete this object."""
        self.deleted = True
        self.deleted_at = timeutils.utcnow()
        self.save(session=session)


class Service(BASE, SGServiceBase):
    """Represents a running service on a host."""

    __tablename__ = 'services'
    id = Column(Integer, primary_key=True)
    host = Column(String(255))  # , ForeignKey('hosts.id'))
    binary = Column(String(255))
    topic = Column(String(255))
    report_count = Column(Integer, nullable=False, default=0)
    disabled = Column(Boolean, default=False)
    disabled_reason = Column(String(255))
    availability_zone = Column(String(255))
    modified_at = Column(DateTime)
    rpc_current_version = Column(String(36))
    rpc_available_version = Column(String(36))


class Replication(BASE, SGServiceBase):
    """Represents a replication."""

    __tablename__ = 'replications'

    id = Column(String(36), primary_key=True, nullable=False)
    user_id = Column(String(36), nullable=False)
    project_id = Column(String(255), nullable=False)
    host = Column(String(255))
    status = Column(String(64))
    display_name = Column(String(255))
    display_description = Column(String(255))
    master_volume = Column(String(36))
    slave_volume = Column(String(36))

    @property
    def name(self):
        return CONF.replication_name_template % self.id


class Checkpoint(BASE, SGServiceBase):
    """Represents a checkpoint."""

    __tablename__ = 'checkpoints'

    id = Column(String(36), primary_key=True, nullable=False)
    user_id = Column(String(36), nullable=False)
    project_id = Column(String(255), nullable=False)
    host = Column(String(255))
    status = Column(String(64))
    display_name = Column(String(255))
    display_description = Column(String(255))
    replication_id = Column(String(36), ForeignKey("replications.id"),
                            nullable=True)

    replication = orm.relationship(
        Replication,
        backref='checkpoints',
        foreign_keys=replication_id,
        primaryjoin='and_('
                    'Replication.id == Checkpoint.replication_id,'
                    'Replication.deleted == False,'
                    'Checkpoint.deleted == False)')

    @property
    def name(self):
        return CONF.checkpoint_name_template % self.id


class Volume(BASE, SGServiceBase):
    """Represents a volumes."""

    __tablename__ = 'volumes'

    id = Column(String(36), primary_key=True, nullable=False)
    user_id = Column(String(36), nullable=False)
    project_id = Column(String(255), nullable=False)
    host = Column(String(255))
    status = Column(String(64))
    previous_status = Column(String(64))
    display_name = Column(String(255))
    display_description = Column(String(255))
    availability_zone = Column(String(255))
    replication_zone = Column(String(255))
    replication_id = Column(String(36), ForeignKey("replications.id"),
                            nullable=True)
    replicate_status = Column(String(64))
    replicate_mode = Column(String(64))
    access_mode = Column(String(64))

    replication = orm.relationship(
        Replication,
        backref='volumes',
        foreign_keys=replication_id,
        primaryjoin='and_('
                    'Replication.id == Volume.replication_id,'
                    'Replication.deleted == False,'
                    'Volume.deleted == False)')

    @property
    def name(self):
        return CONF.volume_name_template % self.id


class Snapshot(BASE, SGServiceBase):
    """Represents a snapshot"""

    __tablename__ = 'snapshots'

    id = Column(String(36), primary_key=True, nullable=False)
    user_id = Column(String(36), nullable=False)
    project_id = Column(String(255), nullable=False)
    host = Column(String(255))
    status = Column(String(64))
    display_name = Column(String(255))
    display_description = Column(String(255))
    checkpoint_id = Column(String(36), ForeignKey("checkpoints.id"),
                           nullable=True)
    destination = Column(String(36))
    availability_zone = Column(String(255))
    volume_id = Column(String(36), ForeignKey('volumes.id'), nullable=False)

    checkpoint = orm.relationship(
        Checkpoint,
        backref='snapshots',
        foreign_keys=checkpoint_id,
        primaryjoin='and_('
                    'Snapshot.checkpoint_id == Checkpoint.id,'
                    'Snapshot.deleted == 0,'
                    'Checkpoint.deleted == 0)')
    volume = orm.relationship(
        Volume,
        backref='snapshots',
        foreign_keys=volume_id,
        primaryjoin='and_('
                    'Snapshot.volume_id == Volume.id,'
                    'Snapshot.deleted == 0,'
                    'Volume.deleted == 0)')

    @property
    def name(self):
        return CONF.snapshot_name_template % self.id


class Backup(BASE, SGServiceBase):
    """Represents a backup."""

    __tablename__ = 'backups'

    id = Column(String(36), primary_key=True, nullable=False)
    user_id = Column(String(36), nullable=False)
    project_id = Column(String(255), nullable=False)
    host = Column(String(255))
    status = Column(String(64))
    display_name = Column(String(255))
    display_description = Column(String(255))
    size = Column(Integer)
    type = Column(String(36))
    destination = Column(String(36))
    availability_zone = Column(String(255))
    volume_id = Column(String(36), nullable=False)

    @property
    def name(self):
        return CONF.backup_name_template % self.id


class VolumeAttachment(BASE, SGServiceBase):
    """Represents a volume attachmend."""

    __tablename__ = 'volume_attachment'

    id = Column(String(36), primary_key=True, nullable=False)
    volume_id = Column(String(36), ForeignKey('volumes.id'), nullable=False)
    instance_uuid = Column(String(36))
    attached_host = Column(String(255))
    mountpoint = Column(String(255))
    attach_time = Column(DateTime)
    detach_time = Column(DateTime)
    attach_status = Column(String(255))
    attach_mode = Column(String(255))

    volume = orm.relationship(
        Volume,
        backref='volume_attachment',
        foreign_keys=volume_id,
        primaryjoin='and_('
                    'VolumeAttachment.volume_id == Volume.id,'
                    'VolumeAttachment.deleted == 0,'
                    'Volume.deleted == 0)')


def register_models():
    """Register Models and create metadata.

    Called from sgservice.db.sqlalchemy.__init__ as part of loading the driver,
    it will never need to be called explicitly elsewhere unless the
    connection is lost and needs to be reestablished.
    """
    from sqlalchemy import create_engine
    models = (Service,
              Replication,
              Checkpoint,
              Volume,
              Snapshot,
              Backup,
              VolumeAttachment)
    engine = create_engine(CONF.database.connection, echo=False)
    for model in models:
        model.metadata.create_all(engine)
