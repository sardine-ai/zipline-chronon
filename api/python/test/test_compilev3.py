import os

from ai.chronon.repo.compilev3 import __compile_v3


def test_compile(repo):
    os.chdir(repo)
    results = __compile_v3(chronon_root=repo)
    assert len(results) != 0
