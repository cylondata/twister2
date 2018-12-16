import re, os

configs = {

    # AURORA
    # "aurora_client": {
    #     "title": "Aurora Client Configuration",
    #     "description": "",
    #     "yml": "twister2/config/src/yaml/conf/aurora/client.yaml",
    #     "doc": "docs/configurations/aurora/client.md"
    # },

    # K8s
    # "kubernetes_client": {
    #     "title": "Kubernetes Client Configuration",
    #     "description": "",
    #     "yml": "twister2/config/src/yaml/conf/kubernetes/client.yaml",
    #     "doc": "docs/configurations/kubernetes/client.md"
    # },

    # mesos
    # "mesos_client": {
    #     "title": "Mesos Client Configuration",
    #     "description": "",
    #     "yml": "twister2/config/src/yaml/conf/mesos/client.yaml",
    #     "doc": "docs/configurations/mesos/client.md"
    # },
}

# Common config files definitions will be generated here. Please add other configs to above dictionary
modes = ["standalone", "slurm", "aurora", "kubernetes", "mesos", "nomad"]
common_files = ["data", "network", "resource", "system", "task", "uploader"]

descriptions = {
    "standalone_data": ""  # example
}

for mode in modes:
    for file in common_files:
        key = mode + "_" + file
        configs[key] = {}
        configs[key]["title"] = mode.capitalize() + " " + file.capitalize() + " Configuration"
        configs[key]["description"] = "" if not descriptions.has_key(key) else descriptions[key]
        configs[key]["yml"] = "twister2/config/src/yaml/conf/" + mode + "/" + file + ".yaml"
        configs[key]["doc"] = "docs/configurations/" + mode + "/" + file + ".md"


# End of common configuration generation

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
                    current_row.description += "<br/>" + comment_regex.group(1)
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
