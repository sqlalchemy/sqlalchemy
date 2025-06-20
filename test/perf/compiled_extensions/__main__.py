from pathlib import Path
import sys

parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir.absolute()))
if True:
    from compiled_extensions import command


if __name__ == "__main__":
    command.main()
