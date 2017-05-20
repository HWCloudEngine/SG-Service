import base64


def parse_host(host_ip, port, scheme):
    return '{0}://{1}:{2}'.format(scheme, host_ip, port)


def create_container_config(image_name, image_id, root_volume_id = None, network_info=None,
                            block_device_info=None, inject_files=None, admin_password=None):
    result =  {
        'image_name': image_name,
        'image_id': image_id,
        'network_info': network_info,
        'block_device_info': block_device_info,
        'inject_files': inject_files,
        'admin_password': admin_password,
        'root_volume_id': root_volume_id
    }

    return result


def restart_container_config(network_info, block_device_info):
    return {
        'network_info': network_info,
        'block_device_info': block_device_info
    }


def start_container_config(network_info, block_device_info):
    return {
        'network_info': network_info,
        'block_device_info': block_device_info
    }


def inject_file_config(dst_path, src_path, file_data):
    if src_path:
        with open(src_path) as f:
            data = f.read()
        encoded = base64.b64encode(data)
    elif file_data:
        encoded = base64.b64encode(file_data)

    return {
        'dst_path': dst_path,
        'file_data': encoded
    }

def admin_password_config(admin_password):
    return {
        'admin_password': base64.b64encode(admin_password)
    }

def create_image_config(image_name, image_id):
    return {
        'image_name': image_name,
        'image_id': image_id
    }
