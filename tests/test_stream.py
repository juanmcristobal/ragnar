import pytest
import glob
import tempfile

from ragnar.generators import read_file
from ragnar.stream import Stream
from .helpers.utils import generate_mock_file

NUM_FILES = 10
NUM_LINES = 100


@pytest.fixture(scope="session")
def mock_files(tmpdir_factory):
    dirpath = tempfile.TemporaryDirectory()
    mock = {'dirpath': dirpath.name + '/*', 'mockfiles': []}
    for files in range(0, NUM_FILES):
        tmpfile = tempfile.mkstemp(dir=dirpath.name)
        mock['mockfiles'].append(generate_mock_file(tmpfile[1], NUM_LINES,
                                                    {'startswith9': lambda x: x[0] == '9'}))

    # for some filter tests
    mock.setdefault('startswith9', sum(i.startswith9 for i in mock['mockfiles']))
    yield mock
    dirpath.cleanup()


def test_iter_simple_no_repeatable(mock_files):
    stream = Stream(glob.glob(mock_files['dirpath']))
    result = [rst for rst in stream]
    assert (result == glob.glob(mock_files['dirpath']))
    # No repeatable
    result = [rst for rst in stream]
    assert (result == [])


def test_iter_simple_repeatable(mock_files):
    stream = Stream(glob.glob(mock_files['dirpath']), repeatable=True)
    result = [rst for rst in stream]
    assert (result == glob.glob(mock_files['dirpath']))
    # Repeatable
    result = [rst for rst in stream]
    assert (result == glob.glob(mock_files['dirpath']))


def test_single_do_no_repeatable(mock_files):
    stream = Stream(glob.glob(mock_files['dirpath']))
    stream.do(read_file, chain=True)
    assert (len([i for i in stream]) == NUM_FILES * NUM_LINES)
    assert (len([i for i in stream]) == 0)


def test_simple_do_repeatable(mock_files):
    stream = Stream(glob.glob(mock_files['dirpath']), repeatable=True)
    stream.do(read_file, chain=True)
    assert (len([i for i in stream]) == NUM_FILES * NUM_LINES)
    assert (len([i for i in stream]) == NUM_FILES * NUM_LINES)


def test_single_do_no_repeatable_nochain(mock_files):
    stream = Stream(glob.glob(mock_files['dirpath']))
    stream.do(read_file)
    assert (len([i for i in stream]) == NUM_FILES)
    assert (len([i for i in stream]) == 0)

def test_single_do_repeatable_nochain(mock_files):
    stream = Stream(glob.glob(mock_files['dirpath']), repeatable=True)
    stream.do(read_file)
    assert (len([i for i in stream]) == NUM_FILES)
    assert (len([i for i in stream]) == NUM_FILES)

def test_simple_filter_no_repeatable(mock_files):
    stream = Stream(glob.glob(mock_files['dirpath']))
    stream.do(read_file, chain=True)
    stream.filter(lambda line: not line.startswith('9'))
    assert (len([i for i in stream]) == (NUM_FILES * NUM_LINES) - mock_files['startswith9'])
    assert (len([i for i in stream]) == 0)


def test_simple_filter_repeatable(mock_files):
    stream = Stream(glob.glob(mock_files['dirpath']), repeatable=True)
    stream.do(read_file, chain=True)
    stream.filter(lambda line: not line.startswith('9'))
    assert (len([i for i in stream]) == (NUM_FILES * NUM_LINES) - mock_files['startswith9'])
    assert (len([i for i in stream]) == (NUM_FILES * NUM_LINES) - mock_files['startswith9'])
