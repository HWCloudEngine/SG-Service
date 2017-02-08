from sgservice.controller.grpc.control import common_pb2
from sgservice.controller.grpc.control import volume_control_pb2
from sgservice.controller.grpc.control import volume_control_pb2_grpc

STATUS_MAPPING = {
    'available': common_pb2.VOL_AVAILABLE,
    'enabling': common_pb2.VOL_ENABLING,
    'attached': common_pb2.VOL_ATTACHED
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

    def enable_sg(self, volume_id, device, size):
        req = volume_control_pb2.EnableSGReq(volume_id=volume_id,
                                             device=device,
                                             size=size)
        response = self.stub.EnableSG(req)
        if response.status == common_pb2.sOk:
            return {'status': 0, 'driver_data': response.driver_data}
        else:
            return {'status': response.status}

    def disable_sg(self, volume_id):
        req = volume_control_pb2.DisableSGReq(volume_id=volume_id)
        response = self.stub.DisableSG(req)
        return {'status': response.status}

    def get_volume(self, volume_id):
        req = volume_control_pb2.GetVolumeReq(volume_id=volume_id)
        response = self.stub.GetVolume(req)
        if response.status == common_pb2.sOk:
            return {'status': 0, 'volume': response.volume}
        else:
            return {'status': response.status}

    def list_volumes(self):
        req = volume_control_pb2.ListDevicesReq()
        response = self.stub.ListVolumes(req)
        if response.status == common_pb2.sOk:
            return {'status': 0, 'volumes': response.volumes}
        else:
            return {'status': response.status}
