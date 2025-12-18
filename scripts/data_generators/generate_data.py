from scripts.data_generators.tests import IcebergTest
from scripts.data_generators.connections import IcebergConnection
import sys
from typing import Dict

import argparse


parser = argparse.ArgumentParser(description="Generate data for various systems.")
parser.add_argument(
    "targets",
    nargs="+",
    choices=["polaris", "lakekeeper", "local", "spark-rest"],
    help="Specify one or more targets to generate data for",
)
parser.add_argument("--test", help='Generate only a specific test (for debugging)', action='store')

args = parser.parse_args()

test_classes = IcebergTest.registry
tests = []
for test_class in test_classes:
    tests.append(test_class())

if args.test:
    tests = [x for x in tests if x.table == args.test]

for target in args.targets:
    connection_class = IcebergConnection.get_class(target)
    con = connection_class()
    print(f"Generating for '{target}'")
    for test in tests:
        print(f"Generating test '{test.table}'")
        if test.table == "spark_written_upper_lower_bounds":
            continue
        test.generate(con)

if __name__ == "__main__":
    if __package__ is None:
        raise RuntimeError(
            "This script must be run as a module.\n" "Use: python -m scripts.data_generators.generate_data [args]"
        )
