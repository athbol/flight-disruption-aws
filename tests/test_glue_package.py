import ast
from pathlib import Path

ROOT = Path(__file__).parent.parent


def fda_imports(path):
    found = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module == "fda":
            found |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module.startswith("fda."):
            found.add(node.module.removeprefix("fda."))
        elif isinstance(node, ast.Import):
            found |= {a.name.removeprefix("fda.") for a in node.names if a.name.startswith("fda.")}
    return found


def zipped_modules():
    for node in ast.parse((ROOT / "infra" / "__main__.py").read_text()).body:
        if isinstance(node, ast.Assign) and node.targets[0].id == "FDA_MODULES":
            return {name.removesuffix(".py") for name in ast.literal_eval(node.value)}


def test_package_zip_has_every_fda_module_the_job_imports():
    needed = fda_imports(ROOT / "jobs" / "glue_curate.py") | fda_imports(
        ROOT / "src" / "fda" / "curate.py"
    )

    assert needed | {"__init__"} <= zipped_modules()
