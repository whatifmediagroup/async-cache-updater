"""Console script for async_cache_updater."""
import argparse
import sys


def main():
    """Console script for async_cache_updater."""
    parser = argparse.ArgumentParser()
    parser.add_argument('_', nargs='*')
    args = parser.parse_args()

    print("Arguments: " + str(args._))
    print("Replace this message by putting your code into "
          "async_cache_updater.cli.main")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
