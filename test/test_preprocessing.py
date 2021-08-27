import mapchete
import pytest


def _trivial_func(arg, kwarg=None):
    return arg + kwarg + "bar"


def test_preprocessing_empty_tasks_per_input(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        assert len(mp.config.input) == len(mp.config.preprocessing_tasks_per_input())


def test_preprocessing_empty_tasks(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        assert len(mp.config.preprocessing_tasks()) == 0


def test_preprocessing_empty_tasks_count(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        assert mp.config.preprocessing_tasks_count() == 0


def test_preprocessing_set_empty_preprocessing_task_result(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        with pytest.raises(KeyError):
            mp.config.set_preprocessing_task_result("foo", "bar")


def test_add_preprocessing_task(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        # get input object
        inp = mp.config.input_at_zoom("file1", 10)
        tasks = mp.count_tasks()
        # add a preprocessing task and make sure it is registered
        inp.add_preprocessing_task(_trivial_func, fargs=("foo"))
        assert len(mp.config.preprocessing_tasks()) == 1
        assert mp.config.preprocessing_tasks_count() == 1
        assert mp.count_tasks() == tasks + 1


def test_run_preprocessing_task(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        # get input object
        inp = mp.config.input_at_zoom("file1", 10)
        with pytest.raises(KeyError):
            inp.get_preprocessing_task_result("test_task")
        # add a preprocessing task and make sure it is registered
        inp.add_preprocessing_task(
            _trivial_func, key="test_task", fargs="foo", fkwargs={"kwarg": "foo"}
        )
        with pytest.raises(ValueError):
            inp.get_preprocessing_task_result("test_task")
        mp.batch_preprocess()
        assert inp.get_preprocessing_task_result("test_task") == "foofoobar"


def test_run_preprocessing_tasks(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        inp1 = mp.config.input_at_zoom("file1", 10)
        inp1.add_preprocessing_task(
            _trivial_func, key="test_task", fargs="foo", fkwargs={"kwarg": "foo"}
        )
        inp1.add_preprocessing_task(
            _trivial_func, key="test_other_task", fargs="bar", fkwargs={"kwarg": "foo"}
        )
        inp2 = mp.config.input_at_zoom("file2", 10)
        inp2.add_preprocessing_task(
            _trivial_func, key="test_task", fargs="foo", fkwargs={"kwarg": "foo"}
        )

        # total number of tasks should be 3
        assert (
            sum(
                [
                    len(tasks)
                    for tasks in mp.config.preprocessing_tasks_per_input().values()
                ]
            )
            == 3
        )

        # number of unique tasks should be 2
        assert mp.config.preprocessing_tasks_count() == 2

        mp.batch_preprocess()
        assert inp1.get_preprocessing_task_result("test_task") == "foofoobar"
        assert inp1.get_preprocessing_task_result("test_other_task") == "barfoobar"
        assert inp2.get_preprocessing_task_result("test_task") == "foofoobar"
