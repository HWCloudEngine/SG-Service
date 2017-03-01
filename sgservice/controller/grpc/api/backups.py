from sgservice.common import constants
from sgservice.controller.grpc.control import backup_pb2
from sgservice.controller.grpc.control import common_pb2
from sgservice.controller.grpc.control import backup_control_pb2
from sgservice.controller.grpc.control import backup_control_pb2_grpc
from sgservice.objects import fields

BACKUP_MODE_MAPPING = {
    constants.FULL_BACKUP: backup_pb2.BACKUP_FULL,
    constants.INCREMENTAL_BACKUP: backup_pb2.BACKUP_INCR
}

BACKUP_STATUS_MAPPING = {
    backup_pb2.BACKUP_CREATING: fields.BackupStatus.CREATING,
    backup_pb2.BACKUP_CREATED: fields.BackupStatus.AVAILABLE,
    backup_pb2.BACKUP_DELETING: fields.BackupStatus.DELETING,
    backup_pb2.BACKUP_DELETED: fields.BackupStatus.DELETED,
    backup_pb2.BACKUP_RESTORING: fields.BackupStatus.RESTORING,
    backup_pb2.BACKUP_RESTORED: fields.BackupStatus.AVAILABLE
}


class BackupClient(object):
    def __init__(self, channel):
        self.stub = backup_control_pb2_grpc.BackupControlStub(channel)

    def create_backup(self, backup):
        vol_name = backup['volume_id']
        vol_size = backup['size']
        backup_name = backup['id']
        backup_option = backup_pb2.BackupOption(
            backup_mode=BACKUP_MODE_MAPPING[backup['type']])

        req = backup_control_pb2.CreateBackupReq(
            vol_name=vol_name,
            vol_size=vol_size,
            backup_name=backup_name,
            backup_option=backup_option)

        response = self.stub.CreateBackup(req)
        return {'status': response.status}

    def delete_backup(self, backup):
        vol_name = backup['volume_id']
        backup_name = backup['id']

        req = backup_control_pb2.DeleteBackupReq(
            vol_name=vol_name,
            backup_name=backup_name)

        response = self.stub.DeleteBackup(req)
        return {'status': response.status}

    def restore_backup(self, backup, volume, device):
        vol_name = backup['volume_id']
        backup_name = backup['id']
        new_vol_name = volume['id']
        new_vol_size = volume['size']

        req = backup_control_pb2.RestoreBackupReq(
            vol_name=vol_name,
            backup_name=backup_name,
            new_vol_name=new_vol_name,
            new_vol_size=new_vol_size,
            new_block_device=device)

        response = self.stub.RestoreBackup(req)
        return {'status': response.status}

    def get_backup(self, backup):
        vol_name = backup['volume_id']
        backup_name = backup['id']

        req = backup_control_pb2.GetBackupReq(
            vol_name=vol_name,
            backup_name=backup_name)

        response = self.stub.GetBackup(req)
        if response.status == common_pb2.sOk:
            backup = {
                'id': backup['id'],
                'status': BACKUP_STATUS_MAPPING[response.backup_status]
            }
            return {'status': 0,
                    'backup': backup}
        else:
            return {'status': response.status}

    def list_backups(self, volume_id):
        req = backup_control_pb2.ListBackupReq(vol_name=volume_id)

        response = self.stub.ListBackup(req)
        if response.status == common_pb2.sOk:
            backups = []
            for item in response.backup_name:
                backups.append({'id': item})
            return {'status': 0, 'backups': backups}
        else:
            return {'status': response.status}
