from sgservice.common import constants
from sgservice.controller.grpc.control import common_pb2
from sgservice.controller.grpc.control import replicate_control_pb2
from sgservice.controller.grpc.control import replicate_control_pb2_grpc

REP_ROLE_MAPPING = {
    constants.REP_MASTER: common_pb2.REP_PRIMARY,
    constants.REP_SLAVE: common_pb2.REP_SECONDARY
}


class ReplicateClient(object):
    def __init__(self, channel):
        self.stub = replicate_control_pb2_grpc.ReplicateControlStub(channel)

    def create_replicate(self, volume):
        rep_uuid = volume['replication_id']
        local_volume = volume['id']
        peer_volumes = [volume['peer_volume']]
        role = REP_ROLE_MAPPING[volume['replicate_mode']]

        req = replicate_control_pb2.CreateReplicationReq(
            rep_uuid=rep_uuid,
            local_volume=local_volume,
            peer_volumes=peer_volumes,
            role=role)

        response = self.stub.CreateReplication(req)
        return {'status': response.status}

    def enable_replicate(self, volume):
        vol_id = volume['id']
        role = REP_ROLE_MAPPING[volume['replicate_mode']]

        req = replicate_control_pb2.EnableReplicationReq(
            vol_id=vol_id,
            role=role)

        response = self.stub.EnableReplication(req)
        return {'status': response.status}

    def disable_replicate(self, volume):
        vol_id = volume['id']
        role = REP_ROLE_MAPPING[volume['replicate_mode']]

        req = replicate_control_pb2.DisableReplicationReq(
            vol_id=vol_id,
            role=role)

        response = self.stub.DisableReplication(req)
        return {'status': response.status}

    def failover_replicate(self, volume):
        vol_id = volume['id']
        role = REP_ROLE_MAPPING[volume['replicate_mode']]

        req = replicate_control_pb2.DisableReplicationReq(
            vol_id=vol_id,
            role=role)

        response = self.stub.FailoverReplication(req)
        return {'status': response.status}

    def reverse_replicate(self, volume):
        vol_id = volume['id']
        role = REP_ROLE_MAPPING[volume['replicate_mode']]

        req = replicate_control_pb2.DisableReplicationReq(
            vol_id=vol_id,
            role=role)

        response = self.stub.ReverseReplication(req)
        return {'status': response.status}

    def delete_replicate(self, volume):
        vol_id = volume['id']
        role = REP_ROLE_MAPPING[volume['replicate_mode']]

        req = replicate_control_pb2.DisableReplicationReq(
            vol_id=vol_id,
            role=role)

        response = self.stub.DeleteReplication(req)
        return {'status': response.status}
