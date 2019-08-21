import re, os

configs = []

# Common config files definitions will be generated here. Please add other configs to above dictionary
modes = ["common", "standalone", "slurm", "aurora", "kubernetes", "mesos", "nomad"]
common_files = ["checkpoint", "data", "network", "resource", "core", "task"]

descriptions = {
    "standalone_data": ""  # example
}

doc_path = "docs/docs/configurations/configurations.md"
md_file = open(doc_path, "w")
md_file.write(
    "---\nid: configurations\ntitle: Twister2 Configurations\nsidebar_label: Configurations\n---\n")

for mode in modes:
    for file in common_files:
        key = mode + "_" + file
        config = {}
        config["type"] = mode
        config["title"] = file.capitalize() + " Configurations"
        config["description"] = "" if not descriptions.has_key(key) else descriptions[key]
        config["yml"] = "twister2/config/src/yaml/conf/" + mode + "/" + file + ".yaml"
        configs.append(config)
        # configs[key]["doc"] = "docs/configurations/" + mode + "/" + file + ".md"


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
    yml_file = os.path.join(twister2_root, config_dic["yml"])

    if not os.path.exists(yml_file):
        return []

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
                if current_row.description == "":
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
    md = "### " + config["title"] + "\n\n"
    md += config["description"] + "\n\n"
    rows_written = 0
    need_header = True
    tclose_needed = False
    for row in rows:
        if row.isTitle:
            # first close the table
            if tclose_needed:
                md += "</tbody></table>\n\n"
                tclose_needed = False

            md += "#### " + row.description + "\n"
            need_header = True
        else:
            if need_header:
                md += "<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead>"
                need_header = False
                tclose_needed = True

            md += "<tbody><tr>"
            md += "<td>" + row.property + "</td>"
            md += "<td>" + row.default_value + "</td>"
            if len(row.value_options) != 0:
                md += "<table><thead><tr><td>Options</td></tr></thead><tbody>"
                for option in row.value_options:
                    md += "<tr><td>" + option + "</td></tr>"
                    first_option = False
                md += "</tbody></table>"
            md += "<td>" + row.description.strip() + "</td>"
            # md += "</tbody></table>\n\n"
            rows_written = rows_written + 1
    md_file.write(md)
    if rows_written == 0:
        md_file.write("No specific configurations\n")

    if tclose_needed:
        md_file.write("</tbody></table>\n\n")


previous_type = ""

for config in configs:
    if not previous_type == config["type"]:
        previous_type = config["type"]
        md_file.write("## " + config["type"].capitalize() + " configurations\n")
    rows = parse_config(config)
    write_rows(rows, config)

md_file.close()
