import argparse
from glob import glob
import os
from pathlib import Path
import shutil
import subprocess as sp
from tempfile import mkdtemp
from textwrap import dedent
import uuid

from .api import get as tfget
import datalad.api as dapi
from datalad_osf.utils import url_from_key, json_from_url

TEMPLATEFLOW_PROJECT_KEY = "ue5gx"


def get(template=None, **kwargs):
    """
    Fetch one or more files from a particular template.
    $ templateflow get fsLR

    Additional key-value pairs can be specified with `--kwargs`.
    $ templateflow get fsLR --kwargs desc=nomedialwall suffix=dparc
    """
    fls = tfget(template[4:], **kwargs)
    print(
        "Got {} files{}{}".format(
            len(fls), ":\n" if fls else "", "\n".join([str(f) for f in fls if f])
        )
    )


def upload(
    template=None,
    files=None,
    force=False,
    osf_pass=None,
    osf_dest=None,
    message=None,
    **kwargs
):
    """
    Upload files to existing template.

    Files are first uploaded to the OSF repository, and then validated with DataLad.

    $ templateflow upload fsLR --files tpl-fsLR_desc-new_T1w.nii.gz
    """

    # 1) upload to OSF TODO: parallelize upload
    osf_cmd = ["osf", "upload"]
    if osf_pass is None:
        osf_pass = os.getenv("OSF_PASSWORD")
    assert osf_pass, "OSF password is not set"

    if force:
        osf_cmd += ["-f"]
    if files is None or not isinstance(files, list):
        print("No files given to upload")
        return

    for f in files:
        if "*" in f:
            osf_cmd += glob(f)
        else:
            osf_cmd.append(f)

    if osf_dest is None:
        osf_dest = template
    osf_cmd.append(osf_dest)

    proc = sp.run(osf_cmd, stdout=sp.PIPE, stderr=sp.PIPE)
    if proc.returncode != 0:
        raise RuntimeError("OSF upload error")

    # 2) generate link csv
    url = None
    for folder in json_from_url(url_from_key(TEMPLATEFLOW_PROJECT_KEY))["data"]:
        if folder["attributes"]["name"] == template:
            url = folder["links"]["move"]
            break
    if url is None:
        raise RuntimeError("No URL found for template {}".format(template))

    hits = ["name,link"]
    items = json_from_url(url)["data"]
    while items:
        item = items.pop(0)
        # expand folders
        if item["attributes"]["kind"] == "folder":
            items += json_from_url(item["links"]["move"])["data"]
            continue
        name = item["attributes"]["name"]
        if template in name:
            link = item["links"]["download"]
            hits.append(",".join((name, link)))

    output_filename = "tfupload-{}.csv".format(uuid.uuid4())
    Path(output_filename).write_text("\n".join(hits))

    # 3) update DataLad paths
    """
    To do this first, move the local image file into a tmp folder.
    > mv tpl-test/*_atlas-test*.nii.gz ~/tmp/
    Then you add the new urls to DataLad. Add a message
    > datalad addurls new_files.csv '{link}' '{name}' --message 'My test atlases'
    > datalad publish
    """
    tmp = mkdtemp()
    for f in files:
        shutil.copy2(f, tmp)

    dapi.addurls(None, output_filename, '{link}', '{name}', message=message)
    dapi.publish()


def main(pargs=None):
    parser = get_parser()
    pargs = parser.parse_args(pargs)
    if pargs.command is None:
        parser.print_help()
        return

    if not pargs.template.startswith("tpl-"):
        pargs.template = "tpl-%s" % pargs.template

    if pargs.command == "get":
        get(**vars(pargs))
    elif pargs.command == "upload":
        upload(**vars(pargs))


class StoreDictKeyPair(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        self._nargs = nargs
        super(StoreDictKeyPair, self).__init__(
            option_strings, dest, nargs=nargs, **kwargs
        )

    def __call__(self, parser, namespace, values, option_string=None):
        rdict = {}
        for kv in values:
            k, v = kv.split("=")
            rdict[k] = v
        setattr(namespace, self.dest, rdict)


def get_parser():
    desc = dedent(
        """
    TemplateFlow command-line utility.

    Commands:

        get         Fetch template specific files
        upload      Push new templates

    For command specific information, use 'templateflow <command> -h'.
    """
    )

    parser = argparse.ArgumentParser(
        description=desc, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    subparsers = parser.add_subparsers(dest="command")

    def _add_subparser(name, description):
        subp = subparsers.add_parser(
            name,
            description=dedent(description),
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        subp.add_argument("template", help="Target template")
        return subp

    get_parser = _add_subparser("get", get.__doc__)
    get_parser.add_argument(
        "--kwargs",
        action=StoreDictKeyPair,
        nargs="+",
        metavar="KEY=VAL",
        default={},
        help="One or more keyword arguments",
    )
    upload_parser = _add_subparser("upload", upload.__doc__)
    upload_parser.add_argument(
        "--files",
        nargs="+",
        help="Files to upload. Wildcards are accepted and match all characters",
    )
    upload_parser.add_argument(
        "--osf-pass",
        help="OSF password, overrides environmental variable OSF_PASSWORD if set",
    )
    upload_parser.add_argument(
        "--osf-dest", help="OSF output path. If not set, defaults to template name."
    )
    upload_parser.add_argument("--message", help="Message to describe upload")
    upload_parser.add_argument(
        "--force", action="store_true", help="Force upload even if existing files"
    )
    return parser
