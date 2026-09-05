#!/usr/bin/env python3
"""Configure an isolated local ClickHouse instance for the large-scale run."""
import argparse
from pathlib import Path
import xml.etree.ElementTree as ET


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--http-port', type=int, default=18123)
    p.add_argument('--tcp-port', type=int, default=19000)
    args = p.parse_args()
    root = args.root.resolve(); root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        p.error('server root must be empty')
    for name in ('data', 'tmp', 'user_files', 'logs'):
        (root/name).mkdir()
    config = ET.Element('clickhouse')
    def add(parent, name, value):
        node = ET.SubElement(parent, name); node.text = str(value); return node
    logger = ET.SubElement(config, 'logger')
    for name, value in {'level': 'warning', 'log': root/'logs/server.log',
                        'errorlog': root/'logs/error.log', 'size': '100M', 'count': 3}.items():
        add(logger, name, value)
    for name, value in {'http_port': args.http_port, 'tcp_port': args.tcp_port,
                        'listen_host': '127.0.0.1', 'timezone': 'UTC',
                        'path': str(root/'data')+'/', 'tmp_path': str(root/'tmp')+'/',
                        'user_files_path': str(root/'user_files')+'/',
                        'users_config': root/'users.xml', 'default_profile': 'default',
                        'max_server_memory_usage': 184 << 30, 'mark_cache_size': 1 << 30,
                        'uncompressed_cache_size': 0, 'background_pool_size': 16,
                        'background_schedule_pool_size': 16}.items():
        add(config, name, value)
    users = ET.Element('clickhouse')
    profile = ET.SubElement(ET.SubElement(users, 'profiles'), 'default')
    # Loading a Memory table can retain allocations throughout INSERT.
    # Measured queries override this with their explicit 64 GiB limit.
    add(profile, 'max_memory_usage', 160 << 30); add(profile, 'max_threads', 8)
    user = ET.SubElement(ET.SubElement(users, 'users'), 'default')
    add(user, 'password', ''); add(user, 'profile', 'default'); add(user, 'quota', 'default')
    add(ET.SubElement(user, 'networks'), 'ip', '127.0.0.1')
    ET.SubElement(ET.SubElement(users, 'quotas'), 'default')
    for name, element in (('config.xml', config), ('users.xml', users)):
        ET.indent(element)
        ET.ElementTree(element).write(root/name, encoding='unicode', xml_declaration=True)
    print(root/'config.xml')


if __name__ == '__main__':
    main()
