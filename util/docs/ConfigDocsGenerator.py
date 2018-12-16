import re, os

configs = {
    "standalone_resource": {
        "title": "Standalone Resource Configuration",
        "description": "",
        "yml": "twister2/config/src/yaml/conf/standalone/resource.yaml",
        "doc": "docs/configurations/standalone/resource.md"
    },
    "standalone_data": {
        "title": "Standalone Data Configuration",
        "description": "",
        "yml": "twister2/config/src/yaml/conf/standalone/data.yaml",
        "doc": "docs/configurations/standalone/data.md"
    },
    "standalone_network": {
        "title": "Standalone Network Configuration",
        "description": "",
        "yml": "twister2/config/src/yaml/conf/standalone/network.yaml",
        "doc": "docs/configurations/standalone/network.md"
    },
    "standalone_system": {
        "title": "Standalone System Configuration",
        "description": "",
        "yml": "twister2/config/src/yaml/conf/standalone/system.yaml",
        "doc": "docs/configurations/standalone/system.md"
    },
    "standalone_task": {
        "title": "Standalone Task Configuration",
        "description": "",
        "yml": "twister2/config/src/yaml/conf/standalone/task.yaml",
        "doc": "docs/configurations/standalone/task.md"
    },
    "standalone_uploader": {
        "title": "Standalone Uploader Configuration",
        "description": "",
        "yml": "twister2/config/src/yaml/conf/standalone/uploader.yaml",
        "doc": "docs/configurations/standalone/uploader.md"
    }
}


class TableRow:
    property = None
    default_value = None
    value_options = []
    description = ""
    isTitle = False

    def __init__(self):
        self.value_options = []
        pass


dirname = os.path.dirname(__file__)
twister2_root = os.path.join(dirname, '../../')


def parse_config(config_dic):
    rows = []
    yml_file = os.path.join(twister2_root, config_dic["yml"]);
    with open(yml_file) as f:
        content = f.readlines()
        current_row = TableRow()
        for line in content:
            title_regex = re.match("#+\n", line)
            comment_regex = re.match("#\s(.+)\n", line)
            commented_property_regex = re.match("#\s(.+):\s(.+)\n", line)
            property_regex = re.match("(.+):\s(.+)\n", line)
            if title_regex:
                current_row.isTitle = True
            if line == "\n" and (current_row.property is not None or current_row.isTitle):
                rows.append(current_row)
                current_row = TableRow()
            elif commented_property_regex:
                if current_row.property is None:
                    current_row.property = commented_property_regex.group(1)
                    current_row.default_value = commented_property_regex.group(2)
                else:
                    current_row.value_options.append(commented_property_regex.group(2))
            elif comment_regex:
                if current_row.description is None:
                    current_row.description = comment_regex.group(1)
                else:
                    current_row.description += " " + comment_regex.group(1)
            elif property_regex:
                current_row.property = property_regex.group(1)
                current_row.default_value = property_regex.group(2)
        if current_row.property is not None:
            rows.append(current_row)
    return rows


def write_rows(rows, config):
    md = "# " + config["title"] + "\n\n"
    md += config["description"] + "\n\n"
    for row in rows:
        if row.isTitle:
            md += "### " + row.description + "\n"
        else:
            md += "**" + row.property + "**\n"
            md += "<table>"
            md += "<tr><td>default</td>" + "<td>" + row.default_value + "</td>"
            if len(row.value_options) != 0:
                md += "<tr><td>options</td><td>"
                first_option = True
                for option in row.value_options:
                    if not first_option:
                        md += "<br/>"
                    md += option
                    first_option = False
                md += "</td>"
            md += "<tr><td>description</td>" + "<td>" + row.description.strip() + "</td>"
            md += "</table>\n\n"
        doc_file = os.path.join(twister2_root, config["doc"])
    doc_parent = os.path.dirname(doc_file)
    if not os.path.exists(doc_parent):
        os.makedirs(os.path.dirname(doc_file))
    md_file = open(doc_file, "w")
    md_file.write(md)
    md_file.close()


for config in configs:
    rows = parse_config(configs[config])
    write_rows(rows, configs[config])
