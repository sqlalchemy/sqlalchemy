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
                "Wheel tag {} matches installed version {}.".format(
                    to_check, t
                )
            )
            found = True
            break
if not found:
    print(
        "Wheel tag {} not found in installed version tags {}.".format(
            to_check, [str(t) for t in tags.sys_tags()]
        )
    )
    exit(1)
