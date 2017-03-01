from sgservice.controller.grpc.control import common_pb2
from sgservice.controller.grpc.control import volume_control_pb2
from sgservice.controller.grpc.control import volume_control_pb2_grpc
from sgservice.objects import fields

VOL_STATUS_MAPPING = {
    common_pb2.VOL_AVAILABLE: fields.VolumeStatus.ENABLED,
    common_pb2.VOL_ENABLING: fields.VolumeStatus.ENABLING,
    common_pb2.VOL_UNKNOWN: None
}

REP_STATUS_MAPPING = {
    common_pb2.REP_CREATING: fields.ReplicateStatus.ENABLING,
    common_pb2.REP_ENABLING: fields.ReplicateStatus.ENABLING,
    common_pb2.REP_ENABLED: fields.ReplicateStatus.ENABLED,
    common_pb2.REP_DISABLING: fields.ReplicateStatus.DISABLING,
    common_pb2.REP_DISABLED: fields.ReplicateStatus.DISABLED,
    common_pb2.REP_FAILING_OVER: fields.ReplicateStatus.FAILING_OVER,
    common_pb2.REP_FAILED_OVER: fields.ReplicateStatus.FAILED_OVER,
    common_pb2.REP_REVERSING: fields.ReplicateStatus.REVERSING,
    common_pb2.REP_DELETING: fields.ReplicateStatus.DELETING,
    common_pb2.REP_DELETED: fields.ReplicateStatus.DELETED,
    common_pb2.REP_ERROR: fields.ReplicateStatus.ERROR,
    common_pb2.REP_UNKNOW: None,
}


class VolumeClient(object):
    def __init__(self, channel):
        self.stub = volume_control_pb2_grpc.VolumeControlStub(channel)

    def list_devices(self):
        req = volume_control_pb2.ListDevicesReq()
        response = self.stub.ListDevices(req)

        if response.status == common_pb2.sOk:
            return {'status': 0, 'devices': response.devices}
        else:
            return {'status': response.status}

    def enable_sg(self, volume, device):
        volume_id = volume['id']
        size = volume['size']

        req = volume_control_pb2.EnableSGReq(volume_id=volume_id,
                                             device=device,
                                             size=size)
        response = self.stub.EnableSG(req)
        if response.status == common_pb2.sOk:
            return {'status': 0, 'driver_data': response.driver_data}
        else:
            return {'status': response.status}

    def disable_sg(self, volume):
        req = volume_control_pb2.DisableSGReq(volume_id=volume['id'])
        response = self.stub.DisableSG(req)
        return {'status': response.status}

    def get_volume(self, volume):
        req = volume_control_pb2.GetVolumeReq(volume_id=volume['id'])
        response = self.stub.GetVolume(req)
        if response.status == common_pb2.sOk:
            volume = {
                'id': volume['id'],
                'status': VOL_STATUS_MAPPING[response.volume.vol_status],
                'replicate_status': REP_STATUS_MAPPING[
                    response.volume.rep_status]
            }
            return {'status': 0, 'volume': volume}
        else:
            return {'status': response.status}

    def list_volumes(self):
        req = volume_control_pb2.ListDevicesReq()
        response = self.stub.ListVolumes(req)
        if response.status == common_pb2.sOk:
            volumes = []
            for volume in response.volumes:
                volumes.append({
                    'id': volume.vol_id,
                    'status': VOL_STATUS_MAPPING[volume.vol_status],
                    'replicate_status': REP_STATUS_MAPPING[volume.rep_status]
                })
            return {'status': 0, 'volumes': volumes}
        else:
            return {'status': response.status}
