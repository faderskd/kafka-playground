import os
import sys
import re

KAFKA_PATTERN = re.compile("\$KAFKA_\S+")


def replace_properties_from_env(in_file, out_file):
    environment = dict(os.environ)
    new_config_content_lines = []
    with open(in_file, "r") as f:
        content = f.readlines()
        for line in content:
            match = re.search(KAFKA_PATTERN, line)
            if match:
                matched = match.group(0)
                kafka_property = match.group(0).lstrip("$")
                if kafka_property not in environment:
                    raise RuntimeError(f'Cannot find {kafka_property} env variable')
                new_config_content_lines.append(line.replace(matched, environment[kafka_property]))
            else:
                new_config_content_lines.append(line)

    with open(out_file, "w+") as f:
        f.writelines(new_config_content_lines)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        in_file = 'template.server.properties'
        out_file = 'server.properties'
    else:
        in_file = sys.argv[1]
        out_file = sys.argv[2]
    replace_properties_from_env(in_file, out_file)
