import pytest
import main


def test_process():
    res = main.process('NYSE', '2022-02-24')
    print(res)
