import sys

from packaging import tags

to_check = "--"
found = False
if len(sys.argv) > 1:
    to_check = sys.argv[1]
    for t in tags.sys_tags():
        start = "-".join(str(t).split("-")[:2])
        if to_check.lower() == start:
            print(
                "Wheel tag {0} matches installed version {1}.".format(
                    to_check, t
                )
            )
            found = True
            break
if not found:
    print(
        "Wheel tag {0} not found in installed version tags {1}.".format(
            to_check, [str(t) for t in tags.sys_tags()]
        )
    )
    exit(1)
