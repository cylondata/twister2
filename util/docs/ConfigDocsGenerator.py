import re, os

configs = {
    "standalone": {
        "title": "Standalone Resource Configuration",
        "description": "description",
        "yml": "twister2/config/src/yaml/conf/standalone/resource.yaml",
        "doc": "docs/configurations/standalone/resource.md"
    }
}


class TableRow:
    property = None
    default_value = None
    description = ""

    def __init__(self):
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
            comment_regex = re.match("#\s(.+)\n", line)
            commented_property_regex = re.match("#\s(.+):\s(.+)\n", line)
            property_regex = re.match("(.+):\s(.+)\n", line)
            if line == "\n" and current_row.property is not None:
                rows.append(current_row)
                current_row = TableRow()
            elif commented_property_regex:
                current_row.property = commented_property_regex.group(1)
                current_row.default_value = commented_property_regex.group(2)
            elif comment_regex:
                if current_row.description is None:
                    current_row.description = comment_regex.group(1)
                else:
                    current_row.description += " " + comment_regex.group(1)
            elif property_regex:
                current_row.property = property_regex.group(1)
                current_row.default_value = property_regex.group(2)
    return rows


def write_rows(rows, config):
    md = "# " + config["title"] + "\n\n"
    md += config["description"] + "\n\n"
    for row in rows:
        md += "<h3>" + row.property + "</h3>"
        md += "<table>"
        md += "<tr><td>default</td>" + "<td>" + row.default_value + "</td>"
        md += "<tr><td>description</td>" + "<td>" + row.description + "</td>"
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
